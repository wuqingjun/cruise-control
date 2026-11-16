/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.acl;

import java.util.List;
import org.apache.kafka.common.resource.ResourcePattern;

/**
 * Checks that the brokers serve the ACLs that are in ZooKeeper, i.e. that they received the change notifications and
 * reloaded the resources (and that they use the same ACL root as Cruise Control).
 */
public interface AclSyncVerifier {

  /**
   * Wait until the brokers report {@code expected}, or the timeout passes.
   *
   * @param expected The ACLs in ZooKeeper (all resources, managed or not).
   * @param timeoutMs How long to wait for the brokers to catch up.
   * @return Resources the brokers still report differently after the timeout; empty if they match.
   * @throws InterruptedException If interrupted.
   */
  List<ResourcePattern> verify(AclTree expected, long timeoutMs) throws InterruptedException;
}
