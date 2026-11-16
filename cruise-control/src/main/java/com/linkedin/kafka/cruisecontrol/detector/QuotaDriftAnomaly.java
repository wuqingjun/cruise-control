/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.cruisecontrol.detector.AnomalyType;
import com.linkedin.kafka.cruisecontrol.quota.QuotaDrift;
import com.linkedin.kafka.cruisecontrol.detector.notifier.KafkaAnomalyType;
import java.util.Map;
import java.util.function.Supplier;

import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.ANOMALY_DETECTION_TIME_MS_OBJECT_CONFIG;

/**
 * Quota drift found by quota sync ({@code QuotaSyncManager}): the stored client quotas differ from the deployed quota
 * file. Used only to alert through the configured {@code AnomalyNotifier}
 * (Slack, MS Teams, Alerta, ...); quota sync repairs drift itself when {@code quota.sync.drift.action=REAPPLY}, so
 * {@link #fix()} does nothing.
 */
public class QuotaDriftAnomaly extends KafkaAnomaly {
  /** Config key for the {@link QuotaDrift} in {@link #configure(Map)}. */
  public static final String QUOTA_DRIFT_OBJECT_CONFIG = "quota.drift.object";
  private QuotaDrift _drift;

  /**
   * @param drift What quota sync found.
   * @param detectionTimeMs When it was found.
   * @return A configured anomaly.
   */
  public static QuotaDriftAnomaly of(QuotaDrift drift, long detectionTimeMs) {
    QuotaDriftAnomaly anomaly = new QuotaDriftAnomaly();
    anomaly.configure(Map.of(ANOMALY_DETECTION_TIME_MS_OBJECT_CONFIG, detectionTimeMs, QUOTA_DRIFT_OBJECT_CONFIG, drift));
    return anomaly;
  }

  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);
    _drift = (QuotaDrift) configs.get(QUOTA_DRIFT_OBJECT_CONFIG);
    if (_drift == null) {
      throw new IllegalArgumentException("Missing " + QUOTA_DRIFT_OBJECT_CONFIG + " when creating a quota drift anomaly.");
    }
    // Not about stopping anything: the anomaly handler skips notifying while an execution (e.g. a rebalance) is running
    // unless this is true. It only takes effect in fix(), which does nothing here and is never requested by notifiers.
    _stopOngoingExecution = true;
  }

  /**
   * @return What quota sync found.
   */
  public QuotaDrift drift() {
    return _drift;
  }

  @Override
  public AnomalyType anomalyType() {
    return KafkaAnomalyType.QUOTA_DRIFT;
  }

  /**
   * Quota sync has already re-applied the file (or only reports, by configuration).
   *
   * @return {@code false}: no self-healing fix is started.
   */
  @Override
  public boolean fix() {
    return false;
  }

  @Override
  public Supplier<String> reasonSupplier() {
    return () -> String.format("%s: %s", KafkaAnomalyType.QUOTA_DRIFT, _drift);
  }

  @Override
  public String toString() {
    return _drift.toString();
  }
}
