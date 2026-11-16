/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.acl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeAclsOptions;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Verifies through the admin API: {@code describeAcls} for all ACLs, compared with the expected tree, repeated until they
 * match or the timeout passes.
 * <p>
 * Limitation: the admin client sends {@code describeAcls} to one broker of its choice (the least loaded), so one call
 * checks one broker's cache, not every broker's.
 */
public class AdminAclSyncVerifier implements AclSyncVerifier {
  private static final Logger LOG = LoggerFactory.getLogger(AdminAclSyncVerifier.class);
  private static final long RETRY_BACKOFF_MS = 500;

  private final Admin _admin;
  private final Time _time;

  /**
   * @param admin Admin client of Cruise Control.
   * @param time Time.
   */
  public AdminAclSyncVerifier(Admin admin, Time time) {
    _admin = admin;
    _time = time;
  }

  @Override
  public List<ResourcePattern> verify(AclTree expected, long timeoutMs) throws InterruptedException {
    long deadline = _time.milliseconds() + timeoutMs;
    List<ResourcePattern> different;
    while (true) {
      long remaining = Math.max(1, deadline - _time.milliseconds());
      try {
        different = differences(describeAll(remaining), expected);
      } catch (ExecutionException | TimeoutException e) {
        LOG.debug("describeAcls failed; retrying.", e);
        different = null;
      }
      if (different != null && different.isEmpty()) {
        return different;
      }
      if (_time.milliseconds() + RETRY_BACKOFF_MS > deadline) {
        break;
      }
      _time.sleep(RETRY_BACKOFF_MS);
    }
    if (different == null) {
      throw new IllegalStateException("Couldn't read the brokers' ACLs (describeAcls) within " + timeoutMs + " ms.");
    }
    return different;
  }

  /**
   * @param timeoutMs Request timeout.
   * @return All ACLs a broker reports, as a tree.
   * @throws ExecutionException If the request failed.
   * @throws TimeoutException If it timed out.
   * @throws InterruptedException If interrupted.
   */
  AclTree describeAll(long timeoutMs) throws ExecutionException, TimeoutException, InterruptedException {
    Collection<AclBinding> bindings = _admin.describeAcls(AclBindingFilter.ANY, new DescribeAclsOptions().timeoutMs((int) timeoutMs))
                                            .values().get(timeoutMs, TimeUnit.MILLISECONDS);
    return toTree(bindings);
  }

  /**
   * @param bindings ACL bindings from the admin API.
   * @return The same ACLs as a tree (only literal and prefixed patterns exist in Kafka's storage).
   */
  static AclTree toTree(Collection<AclBinding> bindings) {
    Map<ResourcePattern, List<AclEntryKey>> byResource = new HashMap<>();
    for (AclBinding binding : bindings) {
      PatternType patternType = binding.pattern().patternType();
      if (patternType != PatternType.LITERAL && patternType != PatternType.PREFIXED) {
        continue;
      }
      byResource.computeIfAbsent(binding.pattern(), p -> new ArrayList<>()).add(AclEntryKey.of(binding.entry()));
    }
    AclTree tree = new AclTree();
    byResource.forEach(tree::put);
    return tree;
  }

  /**
   * @param actual What a broker reports.
   * @param expected What ZooKeeper has.
   * @return Resources that differ.
   */
  static List<ResourcePattern> differences(AclTree actual, AclTree expected) {
    List<ResourcePattern> different = new ArrayList<>();
    AclTreeDiff.diff(actual, expected, Set.of()).changes().forEach(c -> different.add(c.pattern()));
    return different;
  }
}
