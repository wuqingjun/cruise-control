/*
 * Copyright 2026 qjwu. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.quota;

import java.util.List;

/**
 * Drift found while the quota file was unchanged: the stored quotas differ from the file (e.g. changed with
 * {@code kafka-configs.sh} by someone allowed to, or written directly in ZooKeeper).
 */
public final class QuotaDrift {
  private final String _sha256;
  private final List<QuotaDiff.Change> _changes;
  private final boolean _reapplied;

  /**
   * @param sha256 Hash of the approved file.
   * @param changes What it takes to bring the stored quotas back to the file.
   * @param reapplied Whether Cruise Control repaired it ({@code quota.sync.drift.action=REAPPLY}).
   */
  public QuotaDrift(String sha256, List<QuotaDiff.Change> changes, boolean reapplied) {
    _sha256 = sha256;
    _changes = List.copyOf(changes);
    _reapplied = reapplied;
  }

  /**
   * @return Hash of the approved file.
   */
  public String sha256() {
    return _sha256;
  }

  /**
   * @return Differences between the stored quotas and the file.
   */
  public List<QuotaDiff.Change> changes() {
    return _changes;
  }

  /**
   * @return Whether the drift was repaired.
   */
  public boolean reapplied() {
    return _reapplied;
  }

  @Override
  public String toString() {
    return String.format("Quota drift (file sha256 %s, %s): stored vs file %s", _sha256, _reapplied ? "re-applied" : "reported only",
                         _changes);
  }
}
