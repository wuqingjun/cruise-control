/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.acl;

/**
 * A ZooKeeper batch failed: a node changed since it was read (version check), already existed, or disappeared. The batch
 * was not applied (ZooKeeper multi is atomic); earlier batches of the same apply may have been. The caller restarts the
 * whole process: read ZooKeeper again, compare, apply what is left.
 */
public class AclZkConflictException extends Exception {
  private static final long serialVersionUID = 1L;
  private final int _appliedBatches;

  /**
   * @param message What failed.
   * @param appliedBatches Batches of this apply that were committed before the failure.
   * @param cause The ZooKeeper error.
   */
  public AclZkConflictException(String message, int appliedBatches, Throwable cause) {
    super(message, cause);
    _appliedBatches = appliedBatches;
  }

  /**
   * @return Batches of this apply committed before the failure.
   */
  public int appliedBatches() {
    return _appliedBatches;
  }
}
