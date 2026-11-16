/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.cruisecontrol.detector.AnomalyType;
import com.linkedin.kafka.cruisecontrol.acl.AclDrift;
import com.linkedin.kafka.cruisecontrol.detector.notifier.KafkaAnomalyType;
import java.util.Map;
import java.util.function.Supplier;

import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.ANOMALY_DETECTION_TIME_MS_OBJECT_CONFIG;

/**
 * ACL drift found by ACL sync ({@code AclSyncManager}): ZooKeeper differs from the deployed ACL file, or the brokers
 * serve ACLs that differ from ZooKeeper. Used only to alert through the configured {@code AnomalyNotifier}
 * (Slack, MS Teams, Alerta, ...); ACL sync repairs drift itself when {@code acl.sync.drift.action=REAPPLY}, so
 * {@link #fix()} does nothing.
 */
public class AclDriftAnomaly extends KafkaAnomaly {
  /** Config key for the {@link AclDrift} in {@link #configure(Map)}. */
  public static final String ACL_DRIFT_OBJECT_CONFIG = "acl.drift.object";
  private AclDrift _drift;

  /**
   * @param drift What ACL sync found.
   * @param detectionTimeMs When it was found.
   * @return A configured anomaly.
   */
  public static AclDriftAnomaly of(AclDrift drift, long detectionTimeMs) {
    AclDriftAnomaly anomaly = new AclDriftAnomaly();
    anomaly.configure(Map.of(ANOMALY_DETECTION_TIME_MS_OBJECT_CONFIG, detectionTimeMs, ACL_DRIFT_OBJECT_CONFIG, drift));
    return anomaly;
  }

  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);
    _drift = (AclDrift) configs.get(ACL_DRIFT_OBJECT_CONFIG);
    if (_drift == null) {
      throw new IllegalArgumentException("Missing " + ACL_DRIFT_OBJECT_CONFIG + " when creating an ACL drift anomaly.");
    }
    // Not about stopping anything: the anomaly handler skips notifying while an execution (e.g. a rebalance) is running
    // unless this is true. It only takes effect in fix(), which does nothing here and is never requested by notifiers.
    _stopOngoingExecution = true;
  }

  /**
   * @return What ACL sync found.
   */
  public AclDrift drift() {
    return _drift;
  }

  @Override
  public AnomalyType anomalyType() {
    return KafkaAnomalyType.ACL_DRIFT;
  }

  /**
   * ACL sync has already re-applied the file (or only reports, by configuration).
   *
   * @return {@code false}: no self-healing fix is started.
   */
  @Override
  public boolean fix() {
    return false;
  }

  @Override
  public Supplier<String> reasonSupplier() {
    return () -> String.format("%s: %s", KafkaAnomalyType.ACL_DRIFT, _drift);
  }

  @Override
  public String toString() {
    return _drift.toString();
  }
}
