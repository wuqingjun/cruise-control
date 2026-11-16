/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.acl;

/**
 * Receives drift reports from {@link AclSyncManager}; step 5 turns them into an {@code ACL_DRIFT} anomaly so the configured
 * notifier alerts.
 */
@FunctionalInterface
public interface AclDriftListener {
  /** Only logs (the manager already logs the drift at WARN). */
  AclDriftListener LOG_ONLY = drift -> { };

  /**
   * @param drift What differs, and whether it was repaired.
   */
  void onDrift(AclDrift drift);
}
