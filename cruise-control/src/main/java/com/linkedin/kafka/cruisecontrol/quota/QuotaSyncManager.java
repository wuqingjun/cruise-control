/*
 * Copyright 2026 qjwu. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.quota;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.linkedin.kafka.cruisecontrol.acl.AclFileException;
import com.linkedin.kafka.cruisecontrol.acl.AclFileReader;
import com.linkedin.kafka.cruisecontrol.common.KafkaCruiseControlThreadFactory;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.QuotaSyncConfig;
import java.nio.file.Path;
import java.security.PublicKey;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Keeps Kafka's client quotas equal to the deployed, signed quota file (design {@code quota-sync-design.md}), on its own
 * single-thread scheduler, like ACL sync:
 * <ul>
 *   <li><b>File check</b> every {@code quota.sync.check.interval.ms}: if the file's SHA-256 differs from the last applied
 *       one (kept in memory, decision C2), verify the signature, parse and validate, read the stored quotas, apply the
 *       difference.</li>
 *   <li><b>Drift check</b> every {@code quota.sync.drift.check.interval.ms}: compare the stored quotas with the applied
 *       file; report drift to the {@link QuotaDriftListener} and, with {@code REAPPLY}, apply the file again.</li>
 * </ul>
 * The file owns all quotas of entity type {@code user} (decision C3); other entries are left alone and logged once. A
 * missing, unsigned or invalid file is never applied and never removes quotas.
 */
public class QuotaSyncManager implements AutoCloseable {
  /** Metric group of the gauges. */
  public static final String QUOTA_SYNC_SENSOR = "QuotaSync";
  static final long REQUEST_TIMEOUT_MS = 30_000;
  private static final Logger LOG = LoggerFactory.getLogger(QuotaSyncManager.class);

  /** Result of one check. */
  public enum Outcome {
    /** No file deployed: nothing applied. */
    NO_FILE,
    /** File unchanged since the last apply. */
    UNCHANGED,
    /** File unreadable, signature missing or wrong, or content invalid: nothing applied. */
    INVALID_FILE,
    /** Applied (or already equal). */
    APPLIED,
    /** Drift check: no drift. */
    IN_SYNC,
    /** Drift check: drift found and reported. */
    DRIFT_REPORTED,
    /** Drift check: drift found, reported and repaired. */
    DRIFT_REAPPLIED,
    /** Kafka error (e.g. brokers unreachable, change refused): retried at the next check. */
    ERROR
  }

  /** Settings (from {@link QuotaSyncConfig}). */
  public static final class Settings {
    private final long _checkIntervalMs;
    private final long _driftCheckIntervalMs;
    private final boolean _driftReapply;
    private final List<String> _principalTypes;

    /**
     * @param checkIntervalMs File check interval.
     * @param driftCheckIntervalMs Drift check interval.
     * @param driftReapply Whether drift is repaired ({@code REAPPLY}) or only reported.
     * @param principalTypes Allowed identity types in the file.
     */
    public Settings(long checkIntervalMs, long driftCheckIntervalMs, boolean driftReapply, List<String> principalTypes) {
      _checkIntervalMs = checkIntervalMs;
      _driftCheckIntervalMs = driftCheckIntervalMs;
      _driftReapply = driftReapply;
      _principalTypes = List.copyOf(principalTypes);
    }
  }

  @FunctionalInterface
  private interface Check {
    Outcome run() throws Exception;
  }

  private final Settings _settings;
  private final AclFileReader _reader;
  private final QuotaStore _store;
  private final QuotaDriftListener _driftListener;
  private final Time _time;
  private final ScheduledExecutorService _scheduler;
  private volatile Outcome _lastOutcome;
  private volatile String _lastError;
  private final AtomicLong _lastAppliedMs = new AtomicLong(-1);
  private final AtomicLong _failures = new AtomicLong();
  private final AtomicLong _drifts = new AtomicLong();
  // last applied file (in memory only, decision C2)
  private String _appliedSha;
  private volatile QuotaSet _applied;
  // log each distinct problem once while it persists
  private boolean _noFileLogged;
  private String _lastLoggedProblem;
  private List<String> _lastIgnored = List.of();

  /**
   * @param settings Settings.
   * @param reader Reads and verifies the signed file (same format and rules as the ACL file's signature).
   * @param store Kafka's quota storage.
   * @param driftListener Receives drift reports.
   * @param time Time.
   */
  public QuotaSyncManager(Settings settings, AclFileReader reader, QuotaStore store, QuotaDriftListener driftListener, Time time) {
    _settings = settings;
    _reader = reader;
    _store = store;
    _driftListener = driftListener;
    _time = time;
    _scheduler = Executors.newSingleThreadScheduledExecutor(new KafkaCruiseControlThreadFactory("QuotaSyncManager", true, LOG));
  }

  /**
   * Build the manager from Cruise Control's config; fails fast on a bad configuration.
   *
   * @param config Cruise Control config.
   * @param admin Admin client (its principal must be allowed to change quotas).
   * @param metricRegistry Registry for the gauges, or {@code null}.
   * @param driftListener Receives drift reports.
   * @param time Time.
   * @return The manager (not started).
   */
  public static QuotaSyncManager create(KafkaCruiseControlConfig config, Admin admin, MetricRegistry metricRegistry,
                                        QuotaDriftListener driftListener, Time time) {
    String file = config.getString(QuotaSyncConfig.QUOTA_SYNC_FILE_CONFIG);
    if (file == null || file.isEmpty()) {
      throw new ConfigException(QuotaSyncConfig.QUOTA_SYNC_FILE_CONFIG + " must be set when quota.sync.enabled=true.");
    }
    boolean required = config.getBoolean(QuotaSyncConfig.QUOTA_SYNC_SIGNATURE_REQUIRED_CONFIG);
    String keyFile = config.getString(QuotaSyncConfig.QUOTA_SYNC_SIGNATURE_PUBLIC_KEY_FILE_CONFIG);
    PublicKey key = null;
    if (keyFile != null && !keyFile.isEmpty()) {
      try {
        key = AclFileReader.loadPublicKey(Path.of(keyFile));
      } catch (AclFileException e) {
        throw new ConfigException(QuotaSyncConfig.QUOTA_SYNC_SIGNATURE_PUBLIC_KEY_FILE_CONFIG + ": " + e.getMessage());
      }
    } else if (required) {
      throw new ConfigException(QuotaSyncConfig.QUOTA_SYNC_SIGNATURE_PUBLIC_KEY_FILE_CONFIG + " must be set when "
                                + QuotaSyncConfig.QUOTA_SYNC_SIGNATURE_REQUIRED_CONFIG + "=true.");
    }
    String signatureFile = config.getString(QuotaSyncConfig.QUOTA_SYNC_SIGNATURE_FILE_CONFIG);
    AclFileReader reader = new AclFileReader(Path.of(file), signatureFile == null || signatureFile.isEmpty() ? null
                                                                                                         : Path.of(signatureFile),
                                             key, required);
    Settings settings = new Settings(config.getLong(QuotaSyncConfig.QUOTA_SYNC_CHECK_INTERVAL_MS_CONFIG),
                                     config.getLong(QuotaSyncConfig.QUOTA_SYNC_DRIFT_CHECK_INTERVAL_MS_CONFIG),
                                     "REAPPLY".equals(config.getString(QuotaSyncConfig.QUOTA_SYNC_DRIFT_ACTION_CONFIG)),
                                     config.getList(QuotaSyncConfig.QUOTA_SYNC_PRINCIPAL_TYPES_CONFIG));
    LOG.info("Quota sync: file {}, signature {} ({}).", file, reader.signatureFile(), required ? "required" : "NOT required");
    QuotaSyncManager manager = new QuotaSyncManager(settings, reader, new AdminQuotaStore(admin, REQUEST_TIMEOUT_MS), driftListener,
                                                    time);
    if (metricRegistry != null) {
      manager.registerMetrics(metricRegistry);
    }
    return manager;
  }

  private void registerMetrics(MetricRegistry registry) {
    registry.register(MetricRegistry.name(QUOTA_SYNC_SENSOR, "last-applied-time-ms"), (Gauge<Long>) _lastAppliedMs::get);
    registry.register(MetricRegistry.name(QUOTA_SYNC_SENSOR, "failed-checks"), (Gauge<Long>) _failures::get);
    registry.register(MetricRegistry.name(QUOTA_SYNC_SENSOR, "drifts"), (Gauge<Long>) _drifts::get);
    registry.register(MetricRegistry.name(QUOTA_SYNC_SENSOR, "in-sync"), (Gauge<Integer>) () -> isGood(_lastOutcome) ? 1 : 0);
  }

  private static boolean isGood(Outcome outcome) {
    return outcome == Outcome.UNCHANGED || outcome == Outcome.APPLIED || outcome == Outcome.IN_SYNC
           || outcome == Outcome.DRIFT_REAPPLIED;
  }

  /**
   * Start both schedules; the first file check runs right away.
   */
  public void start() {
    _scheduler.scheduleWithFixedDelay(() -> runSafely(this::checkFile), 0, _settings._checkIntervalMs, TimeUnit.MILLISECONDS);
    _scheduler.scheduleWithFixedDelay(() -> runSafely(this::checkDrift), _settings._driftCheckIntervalMs,
                                      _settings._driftCheckIntervalMs, TimeUnit.MILLISECONDS);
    LOG.info("Quota sync started: file check every {} ms, drift check every {} ms ({}).", _settings._checkIntervalMs,
             _settings._driftCheckIntervalMs, _settings._driftReapply ? "REAPPLY" : "REPORT");
  }

  /**
   * Stop the schedules.
   */
  public void shutdown() {
    _scheduler.shutdownNow();
    try {
      if (!_scheduler.awaitTermination(10, TimeUnit.SECONDS)) {
        LOG.warn("Quota sync didn't stop within 10 seconds.");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public void close() {
    shutdown();
  }

  private void runSafely(Check check) {
    try {
      check.run();
    } catch (Throwable t) {
      LOG.error("Quota sync check failed unexpectedly.", t);
    }
  }

  /**
   * @return Outcome of the last check, or {@code null} before the first one.
   */
  public Outcome lastOutcome() {
    return _lastOutcome;
  }

  /**
   * @return Error message of the last check, or {@code null} if it succeeded.
   */
  public String lastError() {
    return _lastError;
  }

  /**
   * File check: apply the file if it changed since the last apply (or nothing has been applied by this instance yet).
   *
   * @return What happened.
   */
  public synchronized Outcome checkFile() {
    return record(() -> {
      Optional<AclFileReader.AclFile> file = _reader.read();
      if (file.isEmpty()) {
        if (!_noFileLogged) {
          LOG.warn("Quota file not found ({}); nothing is applied, stored quotas are unchanged.", _reader.aclFile());
          _noFileLogged = true;
        }
        return Outcome.NO_FILE;
      }
      _noFileLogged = false;
      if (file.get().sha256().equals(_appliedSha)) {
        return Outcome.UNCHANGED;
      }
      QuotaSet desired = load(file.get());
      List<QuotaDiff.Change> changes = sync(desired);
      _appliedSha = file.get().sha256();
      _applied = desired;
      _lastAppliedMs.set(_time.milliseconds());
      LOG.info("Quota file {} (sha256 {}) applied: {} identity change(s), {} identities in the file.", file.get().path(),
               _appliedSha, changes.size(), desired.size());
      return Outcome.APPLIED;
    });
  }

  /**
   * Drift check: compare the stored quotas with the applied file.
   *
   * @return What happened.
   */
  public synchronized Outcome checkDrift() {
    if (_applied == null) {
      return checkFile();
    }
    Outcome outcome = record(() -> {
      Optional<AclFileReader.AclFile> file = _reader.read();
      if (file.isPresent() && !file.get().sha256().equals(_appliedSha)) {
        // a new file: the file check's job
        return null;
      }
      QuotaStore.Stored stored = readStored();
      List<QuotaDiff.Change> changes = QuotaDiff.diff(stored.userQuotas(), _applied);
      if (changes.isEmpty()) {
        return Outcome.IN_SYNC;
      }
      _drifts.incrementAndGet();
      QuotaDrift drift = new QuotaDrift(_appliedSha, changes, _settings._driftReapply);
      LOG.warn("{}", drift);
      _driftListener.onDrift(drift);
      if (!_settings._driftReapply) {
        return Outcome.DRIFT_REPORTED;
      }
      _store.apply(changes);
      audit(changes);
      return Outcome.DRIFT_REAPPLIED;
    });
    if (outcome != null) {
      return outcome;
    }
    Outcome fileOutcome = checkFile();
    if (fileOutcome == Outcome.INVALID_FILE) {
      reportDriftWhileFileInvalid();
    }
    return fileOutcome;
  }

  /**
   * While a newly deployed file is invalid, nothing is applied or repaired, but drift is still detected: the stored
   * quotas are compared with the last valid (applied) file and any difference is reported (alert only).
   */
  private synchronized void reportDriftWhileFileInvalid() {
    try {
      List<QuotaDiff.Change> changes = QuotaDiff.diff(readStored().userQuotas(), _applied);
      if (changes.isEmpty()) {
        return;
      }
      _drifts.incrementAndGet();
      QuotaDrift drift = new QuotaDrift(_appliedSha, changes, false, true);
      LOG.warn("{}", drift);
      _driftListener.onDrift(drift);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (Exception e) {
      LOG.warn("Quota drift check against the last valid file failed: {}", e.toString());
    }
  }

  /**
   * Read the stored quotas, apply the difference to the desired ones.
   *
   * @param desired The file's quotas.
   * @return The applied changes.
   * @throws Exception On Kafka errors.
   */
  private List<QuotaDiff.Change> sync(QuotaSet desired) throws Exception {
    QuotaStore.Stored stored = readStored();
    List<QuotaDiff.Change> changes = QuotaDiff.diff(stored.userQuotas(), desired);
    _store.apply(changes);
    audit(changes);
    return changes;
  }

  private QuotaStore.Stored readStored() throws Exception {
    QuotaStore.Stored stored = _store.read();
    if (!stored.ignored().isEmpty() && !stored.ignored().equals(_lastIgnored)) {
      LOG.warn("{} stored quota entr(y/ies) are not managed by quota sync and have no effect with MultiIdentityQuotaCallback "
               + "(client-id based): {}", stored.ignored().size(), stored.ignored());
    }
    _lastIgnored = stored.ignored();
    return stored;
  }

  private static void audit(List<QuotaDiff.Change> changes) {
    for (QuotaDiff.Change change : changes) {
      LOG.info("  {}", change);
    }
  }

  private QuotaSet load(AclFileReader.AclFile file) throws AclFileException {
    _reader.verify(file);
    return QuotaFileParser.parse(file.bytes(), _settings._principalTypes);
  }

  /**
   * Run a check and record its outcome; errors are logged (each distinct one once) and turned into an outcome.
   *
   * @param check The check.
   * @return Its outcome ({@code null} passes through for "run the file check instead").
   */
  private Outcome record(Check check) {
    Outcome outcome;
    String error = null;
    try {
      outcome = check.run();
    } catch (AclFileException e) {
      outcome = Outcome.INVALID_FILE;
      error = e.getMessage();
      logOnce("Quota file not applied; stored quotas are unchanged. " + error, null);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      outcome = Outcome.ERROR;
      error = "interrupted";
    } catch (Exception e) {
      outcome = Outcome.ERROR;
      error = e.toString();
      logOnce("Quota sync failed; it will try again at the next check: " + error, e);
    }
    if (outcome == null) {
      return null;
    }
    if (outcome == Outcome.ERROR || outcome == Outcome.INVALID_FILE) {
      _failures.incrementAndGet();
      _lastError = error;
    } else {
      _lastError = null;
      _lastLoggedProblem = null;
    }
    _lastOutcome = outcome;
    return outcome;
  }

  private void logOnce(String message, Exception e) {
    if (message.equals(_lastLoggedProblem)) {
      LOG.debug(message);
    } else {
      _lastLoggedProblem = message;
      if (e == null) {
        LOG.error(message);
      } else {
        LOG.error(message, e);
      }
    }
  }
}
