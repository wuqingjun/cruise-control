/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.acl;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.linkedin.kafka.cruisecontrol.common.KafkaCruiseControlThreadFactory;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.ZKConfigUtils;
import com.linkedin.kafka.cruisecontrol.config.constants.AclSyncConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.security.PublicKey;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.utils.Time;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.client.ZKClientConfig;
import org.apache.zookeeper.data.Id;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Keeps the brokers' ACLs in ZooKeeper equal to the deployed ACL file (design sections 5A, G18–G28), on its own
 * single-thread scheduler (D1):
 * <ul>
 *   <li><b>File check</b> every {@code acl.sync.check.interval.ms}: SHA-256 of the file vs. the state node; if different,
 *       verify the signature, parse, validate, compare with ZooKeeper's actual ACLs and apply the difference
 *       ({@link AclZkStore#apply}), then verify through the brokers.</li>
 *   <li><b>Drift check</b> every {@code acl.sync.drift.check.interval.ms}: with the file unchanged, compare it with
 *       ZooKeeper and ZooKeeper with the brokers; report drift to the {@link AclDriftListener} and, with
 *       {@code REAPPLY}, repair it.</li>
 * </ul>
 * A missing, unreadable, unsigned or invalid file is never applied and never causes ACLs to be deleted (G23). Any failure
 * leaves the state node unchanged, so the next check starts the whole process again (G20).
 */
public class AclSyncManager implements AutoCloseable {
  /** Metric group of the gauges. */
  public static final String ACL_SYNC_SENSOR = "AclSync";
  static final int MAX_APPLY_ATTEMPTS = 3;
  static final int ZK_SESSION_TIMEOUT_MS = 30_000;
  private static final Logger LOG = LoggerFactory.getLogger(AclSyncManager.class);

  /** Result of one check. */
  public enum Outcome {
    /** No file deployed: nothing applied. */
    NO_FILE,
    /** File hash equals the applied one. */
    UNCHANGED,
    /** File unreadable, signature missing or wrong, or content invalid: nothing applied. */
    INVALID_FILE,
    /** The ACL root lacks the folders brokers create: probably the wrong root; nothing applied. */
    WRONG_ROOT,
    /** Another Cruise Control instance holds the lock. */
    LOCKED,
    /** Applied (or already equal) and the brokers report the same ACLs. */
    APPLIED,
    /** Applied, but the brokers didn't report the same ACLs within the timeout. */
    VERIFY_FAILED,
    /** Drift check: no drift. */
    IN_SYNC,
    /** Drift check: drift found and reported. */
    DRIFT_REPORTED,
    /** Drift check: drift found, reported and repaired. */
    DRIFT_REAPPLIED,
    /** ZooKeeper or other error: nothing more applied; retried at the next check. */
    ERROR
  }

  /** Creates a store connected to the ACL root; called again after the ZooKeeper session was lost. */
  @FunctionalInterface
  public interface StoreFactory {
    /**
     * @return A connected store.
     * @throws IOException If ZooKeeper can't be reached.
     */
    AclZkStore create() throws IOException;
  }

  /** Settings of the manager (from {@link AclSyncConfig}). */
  public static final class Settings {
    private final long _checkIntervalMs;
    private final long _driftCheckIntervalMs;
    private final boolean _driftReapply;
    private final List<String> _principalTypes;
    private final long _verifyTimeoutMs;
    private final String _owner;
    private final long _notificationRetentionMs;

    /**
     * @param checkIntervalMs File check interval.
     * @param driftCheckIntervalMs Drift check interval.
     * @param driftReapply Whether drift is repaired ({@code REAPPLY}) or only reported.
     * @param principalTypes Allowed principal types.
     * @param verifyTimeoutMs How long to wait for the brokers.
     * @param owner Lock owner name (host / process).
     */
    public Settings(long checkIntervalMs, long driftCheckIntervalMs, boolean driftReapply, List<String> principalTypes,
                    long verifyTimeoutMs, String owner) {
      this(checkIntervalMs, driftCheckIntervalMs, driftReapply, principalTypes, verifyTimeoutMs, owner,
           AclSyncConfig.DEFAULT_ACL_SYNC_NOTIFICATION_RETENTION_MS);
    }

    /**
     * @param checkIntervalMs File check interval.
     * @param driftCheckIntervalMs Drift check interval.
     * @param driftReapply Whether drift is repaired ({@code REAPPLY}) or only reported.
     * @param principalTypes Allowed principal types.
     * @param verifyTimeoutMs How long to wait for the brokers.
     * @param owner Lock owner name (host / process).
     * @param notificationRetentionMs Age after which Cruise Control deletes change notifications.
     */
    public Settings(long checkIntervalMs, long driftCheckIntervalMs, boolean driftReapply, List<String> principalTypes,
                    long verifyTimeoutMs, String owner, long notificationRetentionMs) {
      _notificationRetentionMs = notificationRetentionMs;
      _checkIntervalMs = checkIntervalMs;
      _driftCheckIntervalMs = driftCheckIntervalMs;
      _driftReapply = driftReapply;
      _principalTypes = List.copyOf(principalTypes);
      _verifyTimeoutMs = verifyTimeoutMs;
      _owner = owner;
    }
  }

  private final Settings _settings;
  private final AclFileReader _reader;
  private final StoreFactory _storeFactory;
  private final AclSyncVerifier _verifier;
  private final AclDriftListener _driftListener;
  private final Time _time;
  private final ScheduledExecutorService _scheduler;
  private AclZkStore _store;
  private volatile Outcome _lastOutcome;
  private volatile String _lastError;
  /** Log each distinct problem once while it persists (checks run every few seconds); repeats go to DEBUG. */
  private boolean _noFileLogged;
  private boolean _znodeAclsEnsured;
  private String _currentSha;
  private String _rejectedSha;
  private String _rejectedError;
  private final AtomicLong _lastAppliedMs = new AtomicLong(-1);
  private final AtomicLong _failures = new AtomicLong();
  private final AtomicLong _drifts = new AtomicLong();
  private volatile int _unmanaged;

  /**
   * @param settings Settings.
   * @param reader Reads and verifies the deployed file.
   * @param storeFactory Connects to the ACL root.
   * @param verifier Checks the brokers after an apply.
   * @param driftListener Receives drift reports.
   * @param time Time.
   */
  public AclSyncManager(Settings settings, AclFileReader reader, StoreFactory storeFactory, AclSyncVerifier verifier,
                        AclDriftListener driftListener, Time time) {
    _settings = settings;
    _reader = reader;
    _storeFactory = storeFactory;
    _verifier = verifier;
    _driftListener = driftListener;
    _time = time;
    _scheduler = Executors.newSingleThreadScheduledExecutor(new KafkaCruiseControlThreadFactory("AclSyncManager", true, LOG));
  }

  /**
   * Build the manager from Cruise Control's config. Fails fast on a bad configuration (e.g. missing file setting or an
   * unusable public key); ZooKeeper is connected lazily at the first check.
   *
   * @param config Cruise Control config.
   * @param admin Admin client (for the post-apply verification).
   * @param metricRegistry Registry for the gauges, or {@code null}.
   * @param driftListener Receives drift reports.
   * @param time Time.
   * @return The manager (not started).
   */
  public static AclSyncManager create(KafkaCruiseControlConfig config, Admin admin, MetricRegistry metricRegistry,
                                      AclDriftListener driftListener, Time time) {
    String file = config.getString(AclSyncConfig.ACL_SYNC_FILE_CONFIG);
    if (file == null || file.isEmpty()) {
      throw new ConfigException(AclSyncConfig.ACL_SYNC_FILE_CONFIG + " must be set when acl.sync.enabled=true.");
    }
    boolean required = config.getBoolean(AclSyncConfig.ACL_SYNC_SIGNATURE_REQUIRED_CONFIG);
    String keyFile = config.getString(AclSyncConfig.ACL_SYNC_SIGNATURE_PUBLIC_KEY_FILE_CONFIG);
    PublicKey key = null;
    if (keyFile != null && !keyFile.isEmpty()) {
      try {
        key = AclFileReader.loadPublicKey(Path.of(keyFile));
      } catch (AclFileException e) {
        throw new ConfigException(AclSyncConfig.ACL_SYNC_SIGNATURE_PUBLIC_KEY_FILE_CONFIG + ": " + e.getMessage());
      }
    } else if (required) {
      throw new ConfigException(AclSyncConfig.ACL_SYNC_SIGNATURE_PUBLIC_KEY_FILE_CONFIG + " must be set when "
                                + AclSyncConfig.ACL_SYNC_SIGNATURE_REQUIRED_CONFIG + "=true.");
    }
    String signatureFile = config.getString(AclSyncConfig.ACL_SYNC_SIGNATURE_FILE_CONFIG);
    AclFileReader reader = new AclFileReader(Path.of(file), signatureFile == null || signatureFile.isEmpty() ? null : Path.of(signatureFile),
                                             key, required);

    String root = aclRoot(config);
    ZKClientConfig zkClientConfig = ZKConfigUtils.zkClientConfigFromKafkaConfig(config);
    int maxBatchBytes = config.getInt(AclSyncConfig.ACL_SYNC_MAX_BATCH_BYTES_CONFIG);
    StoreFactory storeFactory = storeFactory(root, zkClientConfig, maxBatchBytes, znodeAclSettings(config));
    Settings settings = new Settings(config.getLong(AclSyncConfig.ACL_SYNC_CHECK_INTERVAL_MS_CONFIG),
                                     config.getLong(AclSyncConfig.ACL_SYNC_DRIFT_CHECK_INTERVAL_MS_CONFIG),
                                     "REAPPLY".equals(config.getString(AclSyncConfig.ACL_SYNC_DRIFT_ACTION_CONFIG)),
                                     config.getList(AclSyncConfig.ACL_SYNC_PRINCIPAL_TYPES_CONFIG),
                                     config.getLong(AclSyncConfig.ACL_SYNC_VERIFY_TIMEOUT_MS_CONFIG),
                                     ManagementFactory.getRuntimeMXBean().getName(),
                                     config.getLong(AclSyncConfig.ACL_SYNC_NOTIFICATION_RETENTION_MS_CONFIG));
    LOG.info("ACL sync: file {}, signature {} ({}), ACL root {}.", file, reader.signatureFile(),
             required ? "required" : "NOT required", root);
    AclSyncManager manager = new AclSyncManager(settings, reader, storeFactory, new AdminAclSyncVerifier(admin, time), driftListener,
                                                time);
    if (metricRegistry != null) {
      manager.registerMetrics(metricRegistry);
    }
    return manager;
  }

  /**
   * Znode ACL settings: auth scheme and broker identities, or {@code null} if znode ACLs are disabled.
   *
   * @param config Cruise Control config.
   * @return Scheme and broker identities, or null.
   */
  static ZnodeAclSettings znodeAclSettings(KafkaCruiseControlConfig config) {
    if (!config.getBoolean(AclSyncConfig.ACL_SYNC_ZOOKEEPER_ACL_ENABLED_CONFIG)) {
      return null;
    }
    String scheme = config.getString(AclSyncConfig.ACL_SYNC_ZOOKEEPER_AUTH_SCHEME_CONFIG);
    List<Id> brokers = AclZnodePolicy.parseIdentities(config.getString(AclSyncConfig.ACL_SYNC_ZOOKEEPER_BROKER_IDENTITIES_CONFIG),
                                                      scheme);
    if (brokers.isEmpty()) {
      throw new ConfigException(AclSyncConfig.ACL_SYNC_ZOOKEEPER_BROKER_IDENTITIES_CONFIG + " must be set when "
                                + AclSyncConfig.ACL_SYNC_ZOOKEEPER_ACL_ENABLED_CONFIG + "=true.");
    }
    return new ZnodeAclSettings(scheme, brokers);
  }

  /** Auth scheme and broker identities for protecting the ACL subtree. */
  public static final class ZnodeAclSettings {
    private final String _scheme;
    private final List<Id> _brokers;

    /**
     * @param scheme ZooKeeper auth scheme, e.g. {@code x509}.
     * @param brokers The brokers' identities.
     */
    public ZnodeAclSettings(String scheme, List<Id> brokers) {
      _scheme = scheme;
      _brokers = List.copyOf(brokers);
    }
  }

  /**
   * A store factory that connects to the ACL root and, if znode ACLs are enabled, builds the policy from this session's
   * own identity ({@code whoAmI}); refuses to work on a session without that identity (e.g. a plaintext connection).
   *
   * @param root ACL root connect string.
   * @param zkClientConfig ZooKeeper client config (TLS).
   * @param maxBatchBytes Batch limit.
   * @param znodeAcls Znode ACL settings, or {@code null} for open znodes.
   * @return The factory.
   */
  public static StoreFactory storeFactory(String root, ZKClientConfig zkClientConfig, int maxBatchBytes, ZnodeAclSettings znodeAcls) {
    return () -> {
      ZooKeeper zk = AclZkStore.connect(root, zkClientConfig, ZK_SESSION_TIMEOUT_MS, ZK_SESSION_TIMEOUT_MS);
      if (znodeAcls == null) {
        return new AclZkStore(zk, maxBatchBytes);
      }
      try {
        Optional<Id> self = AclZkStore.authenticatedId(zk, znodeAcls._scheme);
        if (self.isEmpty()) {
          throw new IOException(String.format("The ZooKeeper session to %s has no %s identity (is TLS with a client certificate "
                                              + "configured?); znode ACLs can't be set.", root, znodeAcls._scheme));
        }
        AclZnodePolicy policy = AclZnodePolicy.protect(self.get(), znodeAcls._brokers);
        LOG.info("ACL sync ZooKeeper session authenticated as {}:{}; znode ACLs: {}.", self.get().getScheme(), self.get().getId(),
                 policy);
        return new AclZkStore(zk, maxBatchBytes, policy);
      } catch (IOException | RuntimeException e) {
        AclZkStore.closeQuietly(zk);
        throw e;
      } catch (KeeperException e) {
        AclZkStore.closeQuietly(zk);
        throw new IOException("whoAmI failed on " + root, e);
      } catch (InterruptedException e) {
        AclZkStore.closeQuietly(zk);
        Thread.currentThread().interrupt();
        throw new IOException("Interrupted while connecting to " + root, e);
      }
    };
  }

  /**
   * @param config Cruise Control config.
   * @return {@code authorizer.zookeeper.url} if set, otherwise {@code zookeeper.connect} (section 4).
   */
  static String aclRoot(KafkaCruiseControlConfig config) {
    String root = config.getString(AclSyncConfig.AUTHORIZER_ZOOKEEPER_URL_CONFIG);
    if (root == null || root.isEmpty()) {
      root = config.getString(ExecutorConfig.ZOOKEEPER_CONNECT_CONFIG);
    }
    if (root == null || root.isEmpty()) {
      throw new ConfigException("ACL sync needs authorizer.zookeeper.url or zookeeper.connect.");
    }
    return root;
  }

  private void registerMetrics(MetricRegistry registry) {
    registry.register(MetricRegistry.name(ACL_SYNC_SENSOR, "last-applied-time-ms"), (Gauge<Long>) _lastAppliedMs::get);
    registry.register(MetricRegistry.name(ACL_SYNC_SENSOR, "failed-checks"), (Gauge<Long>) _failures::get);
    registry.register(MetricRegistry.name(ACL_SYNC_SENSOR, "drifts"), (Gauge<Long>) _drifts::get);
    registry.register(MetricRegistry.name(ACL_SYNC_SENSOR, "unmanaged-resources"), (Gauge<Integer>) () -> _unmanaged);
    registry.register(MetricRegistry.name(ACL_SYNC_SENSOR, "in-sync"),
                      (Gauge<Integer>) () -> isGood(_lastOutcome) ? 1 : 0);
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
    LOG.info("ACL sync started: file check every {} ms, drift check every {} ms ({}).", _settings._checkIntervalMs,
             _settings._driftCheckIntervalMs, _settings._driftReapply ? "REAPPLY" : "REPORT");
  }

  /**
   * Stop the schedules and close the ZooKeeper connection (which also releases the lock).
   */
  public void shutdown() {
    _scheduler.shutdownNow();
    try {
      if (!_scheduler.awaitTermination(10, TimeUnit.SECONDS)) {
        LOG.warn("ACL sync didn't stop within 10 seconds.");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    synchronized (this) {
      closeStore();
    }
  }

  @Override
  public void close() {
    shutdown();
  }

  @FunctionalInterface
  private interface Check {
    Outcome run() throws Exception;
  }

  private void runSafely(Check check) {
    try {
      check.run();
    } catch (Throwable t) {
      LOG.error("ACL sync check failed unexpectedly.", t);
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
   * File check: apply the file if its hash differs from the applied one.
   *
   * @return What happened.
   */
  public synchronized Outcome checkFile() {
    return record(() -> {
      _currentSha = null;
      Outcome maintenance = maintain(store());
      if (maintenance != null) {
        return maintenance;
      }
      Optional<AclFileReader.AclFile> file = _reader.read();
      if (file.isEmpty()) {
        if (!_noFileLogged) {
          LOG.warn("ACL file not found ({}); nothing is applied, ACLs in ZooKeeper are unchanged.", _reader.aclFile());
          _noFileLogged = true;
        } else {
          LOG.debug("ACL file still not found ({}).", _reader.aclFile());
        }
        return Outcome.NO_FILE;
      }
      _noFileLogged = false;
      _currentSha = file.get().sha256();
      AclZkStore store = store();
      Optional<AclZkStore.VersionedState> state = store.readState();
      if (state.isPresent() && state.get().state().sha256().equals(file.get().sha256())) {
        _unmanaged = state.get().state().unmanaged().size();
        return Outcome.UNCHANGED;
      }
      if (file.get().sha256().equals(_rejectedSha)) {
        LOG.debug("ACL file {} (sha256 {}) was rejected before; checking it again.", file.get().path(), file.get().sha256());
      } else {
        LOG.info("ACL file {} changed (sha256 {} -> {}).", file.get().path(),
                 state.map(s -> s.state().sha256()).orElse("none, first run"), file.get().sha256());
      }
      return sync(store, file.get(), load(file.get()));
    });
  }

  /**
   * Drift check: with the file unchanged, compare it with ZooKeeper and ZooKeeper with the brokers.
   *
   * @return What happened.
   */
  public synchronized Outcome checkDrift() {
    Outcome outcome = record(() -> {
      _currentSha = null;
      Optional<AclFileReader.AclFile> file = _reader.read();
      if (file.isEmpty()) {
        return Outcome.NO_FILE;
      }
      _currentSha = file.get().sha256();
      AclZkStore store = store();
      Optional<AclZkStore.VersionedState> state = store.readState();
      if (state.isEmpty() || !state.get().state().sha256().equals(file.get().sha256())) {
        // Not applied yet: that's the file check's job, do it now.
        return null;
      }
      AclTree desired = load(file.get());
      if (store.policy().isProtecting()) {
        Outcome locked = withLock(store, () -> {
          int changed = store.ensureZnodeAcls();
          if (changed > 0) {
            LOG.warn("ZooKeeper ACLs of {} node(s) in the ACL subtree had been changed (only a ZooKeeper super user can do "
                     + "that); restored.", changed);
          }
          return null;
        });
        if (locked != null) {
          return locked;
        }
      }
      AclZkStore.Snapshot snapshot = store.readCurrent();
      AclTreeDiff.Result zkDiff = AclTreeDiff.diff(snapshot.tree(), desired, state.get().state().unmanaged());
      List<ResourcePattern> brokerDifferences = _verifier.verify(snapshot.tree(), _settings._verifyTimeoutMs);
      if (zkDiff.isEmpty() && brokerDifferences.isEmpty()) {
        return Outcome.IN_SYNC;
      }
      return handleDrift(store, file.get(), desired, zkDiff, brokerDifferences);
    });
    return outcome != null ? outcome : checkFile();
  }

  private Outcome handleDrift(AclZkStore store, AclFileReader.AclFile file, AclTree desired, AclTreeDiff.Result zkDiff,
                              List<ResourcePattern> brokerDifferences) throws Exception {
    _drifts.incrementAndGet();
    AclDrift drift = new AclDrift(file.sha256(), zkDiff.changes(), brokerDifferences, _settings._driftReapply);
    LOG.warn("{}", drift);
    _driftListener.onDrift(drift);
    if (!_settings._driftReapply) {
      return Outcome.DRIFT_REPORTED;
    }
    if (!zkDiff.isEmpty()) {
      // Data nodes and their notifications; resources that only the brokers had wrong get notified below.
      Outcome synced = sync(store, file, desired);
      if (synced != Outcome.APPLIED) {
        return synced;
      }
    }
    Set<ResourcePattern> toNotify = new HashSet<>(brokerDifferences);
    zkDiff.changes().forEach(c -> toNotify.remove(c.pattern()));
    if (!toNotify.isEmpty()) {
      Outcome locked = withLock(store, () -> {
        LOG.info("Re-sending change notifications for {} resource(s) the brokers serve differently from ZooKeeper.",
                 store.notifyResources(toNotify));
        return null;
      });
      if (locked != null) {
        return locked;
      }
      Outcome verified = verify(store);
      if (verified != Outcome.APPLIED) {
        return verified;
      }
    }
    return Outcome.DRIFT_REAPPLIED;
  }

  /**
   * Bring ZooKeeper to the file under the lock, then verify through the brokers.
   *
   * @param store Store.
   * @param file The file (for hash and path).
   * @param desired The parsed, validated file.
   * @return What happened.
   * @throws Exception On ZooKeeper errors or repeated conflicts.
   */
  private Outcome sync(AclZkStore store, AclFileReader.AclFile file, AclTree desired) throws Exception {
    List<String> missing = store.missingStructure();
    if (!missing.isEmpty()) {
      _lastError = "ACL root is missing the folders brokers create (wrong authorizer.zookeeper.url / zookeeper.connect?): " + missing;
      LOG.error(_lastError);
      return Outcome.WRONG_ROOT;
    }
    Outcome locked = withLock(store, () -> {
      applyWithRetries(store, file, desired);
      return null;
    });
    if (locked != null) {
      return locked;
    }
    return verify(store);
  }

  /**
   * @param store Store.
   * @return {@link Outcome#APPLIED} if the brokers report what ZooKeeper has, else {@link Outcome#VERIFY_FAILED}.
   * @throws Exception On ZooKeeper errors.
   */
  private Outcome verify(AclZkStore store) throws Exception {
    AclTree expected = store.readCurrent().tree();
    long startMs = _time.milliseconds();
    List<ResourcePattern> differences = _verifier.verify(expected, _settings._verifyTimeoutMs);
    if (!differences.isEmpty()) {
      _lastError = "Brokers don't report the ACLs in ZooKeeper within " + _settings._verifyTimeoutMs + " ms: " + locations(differences);
      LOG.warn(_lastError);
      return Outcome.VERIFY_FAILED;
    }
    LOG.info("Verified: the brokers report the ACLs in ZooKeeper ({} resource(s), {} ms).", expected.numResources(),
             _time.milliseconds() - startMs);
    return Outcome.APPLIED;
  }

  /**
   * @param store Store.
   * @param action Runs while the lock is held.
   * @return {@link Outcome#LOCKED} if another instance holds the lock, otherwise what {@code action} returned.
   * @throws Exception From the action.
   */
  private Outcome withLock(AclZkStore store, Check action) throws Exception {
    if (!store.tryLock(_settings._owner)) {
      LOG.info("Another Cruise Control instance holds {}; skipping.", AclZkStore.LOCK_NODE);
      return Outcome.LOCKED;
    }
    try {
      return action.run();
    } finally {
      store.unlock();
    }
  }

  /**
   * Compare with ZooKeeper's actual state and apply; on a conflict start over from reading ZooKeeper (G20).
   *
   * @param store Store.
   * @param file File.
   * @param desired Desired ACLs.
   * @throws Exception On ZooKeeper errors, or a conflict on the last attempt.
   */
  private void applyWithRetries(AclZkStore store, AclFileReader.AclFile file, AclTree desired) throws Exception {
    for (int attempt = 1; ; attempt++) {
      AclZkStore.Snapshot snapshot = store.readCurrent();
      Optional<AclZkStore.VersionedState> state = store.readState();
      Set<ResourcePattern> unmanaged = state.isPresent() ? state.get().state().unmanaged()
                                                         : new HashSet<>(AclTreeDiff.resourcesOnlyIn(snapshot.tree(), desired));
      AclTreeDiff.Result diff = AclTreeDiff.diff(snapshot.tree(), desired, unmanaged);
      AclStateNode newState = new AclStateNode(file.sha256(), _time.milliseconds(), file.path().toString(), diff.keptUnmanaged());
      try {
        AclZkStore.ApplyResult result = store.apply(diff.changes(), snapshot, newState, state.map(AclZkStore.VersionedState::version));
        audit(file, diff, result);
        _unmanaged = diff.keptUnmanaged().size();
        _lastAppliedMs.set(newState.appliedAtMs());
        return;
      } catch (AclZkConflictException e) {
        if (attempt >= MAX_APPLY_ATTEMPTS) {
          throw e;
        }
        LOG.warn("ACL apply attempt {} of {} hit a conflict ({}); starting over from ZooKeeper's state.", attempt,
                 MAX_APPLY_ATTEMPTS, e.getMessage());
      }
    }
  }

  /**
   * Audit log of an apply (G15).
   *
   * @param file Applied file.
   * @param diff What changed.
   * @param result ZooKeeper batches.
   */
  private static void audit(AclFileReader.AclFile file, AclTreeDiff.Result diff, AclZkStore.ApplyResult result) {
    LOG.info("ACL file {} (sha256 {}) applied: {}; {}.", file.path(), file.sha256(), diff, result);
    for (AclTreeDiff.ResourceChange change : diff.changes()) {
      LOG.info("  {} {}: +{} -{}", change.type(), AclTreeParser.location(change.pattern()), change.entriesAdded(),
               change.entriesRemoved());
    }
    if (!diff.keptUnmanaged().isEmpty()) {
      LOG.warn("{} resource(s) in ZooKeeper are not in the ACL file and are left unmanaged (they existed before the first "
               + "sync): {}", diff.keptUnmanaged().size(), locations(diff.keptUnmanaged()));
    }
  }

  /**
   * Verify the signature, parse and validate.
   *
   * @param file File.
   * @return Desired ACLs.
   * @throws AclFileException If the file can't be used.
   */
  private AclTree load(AclFileReader.AclFile file) throws AclFileException {
    _reader.verify(file);
    AclTree tree = AclTreeParser.parse(file.bytes());
    List<String> problems = AclValidator.validate(tree, _settings._principalTypes);
    if (!problems.isEmpty()) {
      throw new AclFileException(String.format("%d problem(s) in %s: %s", problems.size(), file.path(), String.join(" ", problems)));
    }
    return tree;
  }

  /**
   * Run a check and record its outcome; errors are logged and turned into an outcome, the state node stays unchanged.
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
      if (_currentSha != null && _currentSha.equals(_rejectedSha) && error.equals(_rejectedError)) {
        LOG.debug("ACL file still not applied: {}", error);
      } else {
        LOG.error("ACL file not applied; ACLs in ZooKeeper are unchanged. {}", error);
      }
      _rejectedSha = _currentSha;
      _rejectedError = error;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      outcome = Outcome.ERROR;
      error = "interrupted";
    } catch (Exception e) {
      outcome = Outcome.ERROR;
      error = e.toString();
      LOG.error("ACL sync failed; it will start over at the next check.", e);
      if (e instanceof KeeperException || e instanceof IOException) {
        closeStore();
      }
    }
    if (outcome == null) {
      return null;
    }
    if (outcome != Outcome.INVALID_FILE) {
      _rejectedSha = null;
      _rejectedError = null;
    }
    if (outcome == Outcome.ERROR || outcome == Outcome.INVALID_FILE || outcome == Outcome.WRONG_ROOT
        || outcome == Outcome.VERIFY_FAILED) {
      _failures.incrementAndGet();
      _lastError = error != null ? error : _lastError;
    } else {
      _lastError = null;
    }
    _lastOutcome = outcome;
    return outcome;
  }

  private AclZkStore store() throws IOException {
    if (_store != null && !_store.isAlive()) {
      closeStore();
    }
    if (_store == null) {
      _store = _storeFactory.create();
    }
    return _store;
  }

  private void closeStore() {
    if (_store != null) {
      _store.close();
      _store = null;
    }
    _znodeAclsEnsured = false;
  }

  /**
   * Housekeeping on every file check, file or not: once per ZooKeeper session make the znode ACLs match the policy
   * (under the lock), then delete expired change notifications.
   *
   * @param store Store.
   * @return {@link Outcome#LOCKED} if the znode ACLs couldn't be checked because another instance holds the lock, else null.
   * @throws Exception On ZooKeeper errors.
   */
  private Outcome maintain(AclZkStore store) throws Exception {
    if (store.policy().isProtecting() && !_znodeAclsEnsured) {
      Outcome locked = withLock(store, () -> {
        int changed = store.ensureZnodeAcls();
        LOG.info("ZooKeeper ACLs of the ACL subtree checked ({} node(s) changed): {}.", changed, store.policy());
        return null;
      });
      if (locked != null) {
        return locked;
      }
      _znodeAclsEnsured = true;
    }
    store.purgeNotifications(_settings._notificationRetentionMs, _time.milliseconds());
    return null;
  }

  private static List<String> locations(Collection<ResourcePattern> patterns) {
    List<String> locations = new ArrayList<>();
    patterns.forEach(p -> locations.add(AclTreeParser.location(p)));
    return locations;
  }
}
