/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.quota;

/**
 * Receives quota drift reports from {@link QuotaSyncManager}; wired to the {@code QUOTA_DRIFT} anomaly so the configured
 * notifier alerts.
 */
@FunctionalInterface
public interface QuotaDriftListener {
  /** Only logs (the manager already logs the drift at WARN). */
  QuotaDriftListener LOG_ONLY = drift -> { };

  /**
   * @param drift What differs, and whether it was repaired.
   */
  void onDrift(QuotaDrift drift);
}
