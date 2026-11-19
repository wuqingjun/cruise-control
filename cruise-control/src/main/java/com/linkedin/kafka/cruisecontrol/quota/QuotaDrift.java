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
  private final boolean _whileFileInvalid;

  /**
   * @param sha256 Hash of the approved file.
   * @param changes What it takes to bring the stored quotas back to the file.
   * @param reapplied Whether Cruise Control repaired it ({@code quota.sync.drift.action=REAPPLY}).
   */
  public QuotaDrift(String sha256, List<QuotaDiff.Change> changes, boolean reapplied) {
    this(sha256, changes, reapplied, false);
  }

  /**
   * @param sha256 SHA-256 of the file the stored quotas were compared with.
   * @param changes What differs.
   * @param reapplied Whether the file was re-applied.
   * @param whileFileInvalid Whether the deployed file is invalid, so the comparison is with the last valid file and
   *                         nothing is repaired.
   */
  public QuotaDrift(String sha256, List<QuotaDiff.Change> changes, boolean reapplied, boolean whileFileInvalid) {
    _sha256 = sha256;
    _changes = List.copyOf(changes);
    _reapplied = reapplied;
    _whileFileInvalid = whileFileInvalid;
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

  /**
   * @return Whether the drift was found while the deployed file is invalid (compared with the last valid file, not repaired).
   */
  public boolean whileFileInvalid() {
    return _whileFileInvalid;
  }

  @Override
  public String toString() {
    return String.format("Quota drift (file sha256 %s, %s): stored vs file %s", _sha256,
                         _whileFileInvalid ? "reported only: the deployed file is invalid, compared with the last valid file, not repaired"
                                           : _reapplied ? "re-applied" : "reported only", _changes);
  }
}
