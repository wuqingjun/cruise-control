/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.acl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.kafka.common.resource.ResourcePattern;

/**
 * Result of comparing the brokers' ACLs with ZooKeeper ({@link AclSyncVerifier}): per broker, the resources it serves
 * differently, and the brokers that couldn't be asked. Empty = every checked broker matches.
 */
public final class AclVerifyResult {
  /** Broker id used when the answer came from a broker the admin client chose ({@code ANY_BROKER} mode). */
  public static final int ANY_BROKER = -1;
  private static final AclVerifyResult OK = new AclVerifyResult(new TreeMap<>(), new TreeMap<>());

  private final SortedMap<Integer, List<ResourcePattern>> _differences;
  private final SortedMap<Integer, String> _unreachable;

  private AclVerifyResult(SortedMap<Integer, List<ResourcePattern>> differences, SortedMap<Integer, String> unreachable) {
    _differences = Collections.unmodifiableSortedMap(differences);
    _unreachable = Collections.unmodifiableSortedMap(unreachable);
  }

  /**
   * @return The result "all brokers match".
   */
  public static AclVerifyResult ok() {
    return OK;
  }

  /**
   * @param differences Resources served differently, by broker id (empty lists are dropped).
   * @param unreachable Brokers that couldn't be asked, with the reason.
   * @return The result.
   */
  public static AclVerifyResult of(Map<Integer, List<ResourcePattern>> differences, Map<Integer, String> unreachable) {
    SortedMap<Integer, List<ResourcePattern>> diffs = new TreeMap<>();
    differences.forEach((broker, patterns) -> {
      if (!patterns.isEmpty()) {
        diffs.put(broker, List.copyOf(patterns));
      }
    });
    return new AclVerifyResult(diffs, new TreeMap<>(unreachable));
  }

  /**
   * @param patterns Resources a broker chosen by the admin client serves differently.
   * @return The result, keyed by {@link #ANY_BROKER}.
   */
  public static AclVerifyResult anyBroker(Collection<ResourcePattern> patterns) {
    return of(Map.of(ANY_BROKER, List.copyOf(patterns)), Map.of());
  }

  /**
   * @return Whether every checked broker matches ZooKeeper.
   */
  public boolean isEmpty() {
    return _differences.isEmpty() && _unreachable.isEmpty();
  }

  /**
   * @return Resources served differently, by broker id (sorted by id).
   */
  public SortedMap<Integer, List<ResourcePattern>> differencesByBroker() {
    return _differences;
  }

  /**
   * @return Brokers that couldn't be asked, with the reason (sorted by id).
   */
  public SortedMap<Integer, String> unreachableBrokers() {
    return _unreachable;
  }

  /**
   * @return All resources some broker serves differently (for re-sending notifications), in tree order.
   */
  public List<ResourcePattern> resources() {
    Set<ResourcePattern> all = new TreeSet<>(AclTree.PATTERN_ORDER);
    _differences.values().forEach(all::addAll);
    return new ArrayList<>(all);
  }

  /**
   * @return e.g. {@code broker 2: [kafka-acl/Topic/orders]; broker 3 unreachable (Connection refused)}, or {@code none}.
   */
  public String describe() {
    if (isEmpty()) {
      return "none";
    }
    List<String> parts = new ArrayList<>();
    _differences.forEach((broker, patterns) -> {
      List<String> locations = new ArrayList<>();
      patterns.forEach(p -> locations.add(AclTreeParser.location(p)));
      parts.add(String.format("%s: %s", brokerName(broker), locations));
    });
    _unreachable.forEach((broker, reason) -> parts.add(String.format("%s unreachable (%s)", brokerName(broker), reason)));
    return String.join("; ", parts);
  }

  private static String brokerName(int broker) {
    return broker == ANY_BROKER ? "a broker" : "broker " + broker;
  }

  @Override
  public String toString() {
    return describe();
  }
}
