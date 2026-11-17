/*
 * Copyright 2026 qjwu. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.acl;

/**
 * Checks that the brokers serve the ACLs that are in ZooKeeper, i.e. that they received the change notifications and
 * reloaded the resources (and that they use the same ACL root as Cruise Control).
 * <ul>
 *   <li>{@link PerBrokerAclSyncVerifier} ({@code acl.sync.verify.mode=PER_BROKER}, default): asks every live broker.</li>
 *   <li>{@link AdminAclSyncVerifier} ({@code ANY_BROKER}): asks the broker the admin client chooses.</li>
 * </ul>
 */
public interface AclSyncVerifier extends AutoCloseable {

  /**
   * Wait until the brokers report {@code expected}, or the timeout passes.
   *
   * @param expected The ACLs in ZooKeeper (all resources, managed or not).
   * @param timeoutMs How long to wait for the brokers to catch up.
   * @return Per broker, the resources it still reports differently after the timeout, and brokers that couldn't be
   *         asked; empty if all match.
   * @throws InterruptedException If interrupted.
   */
  AclVerifyResult verify(AclTree expected, long timeoutMs) throws InterruptedException;

  /**
   * Release resources (connections). Default: nothing.
   */
  @Override
  default void close() {
  }
}
