/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.acl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.utils.SecurityUtils;

/**
 * All ACLs of a cluster, in the same shape as Kafka's ACL storage in ZooKeeper:
 * <pre>
 *   LITERAL  (kafka-acl)                 → resource type → resource name → ACL entries
 *   PREFIXED (kafka-acl-extended/prefixed) → resource type → resource name → ACL entries
 * </pre>
 * Every level is sorted: resource types by their ZooKeeper folder name ({@code Cluster}, {@code DelegationToken}, ...),
 * resource names alphabetically (case-sensitive), entries by {@link AclEntryKey}'s order. A resource is only present if
 * it has at least one entry, like a data node in ZooKeeper.
 */
public final class AclTree {
  /** Pattern types Kafka stores (the two subtrees). */
  public static final List<PatternType> PATTERN_TYPES = List.of(PatternType.LITERAL, PatternType.PREFIXED);
  /** Resource types ordered by the name of their ZooKeeper folder. */
  public static final Comparator<ResourceType> RESOURCE_TYPE_ORDER = Comparator.comparing(SecurityUtils::resourceTypeName);
  /** Resource patterns in tree order: literal before prefixed, then resource type, then name. */
  public static final Comparator<ResourcePattern> PATTERN_ORDER = Comparator
      .comparing((ResourcePattern p) -> PATTERN_TYPES.indexOf(p.patternType()))
      .thenComparing(ResourcePattern::resourceType, RESOURCE_TYPE_ORDER)
      .thenComparing(ResourcePattern::name);

  private final Map<PatternType, SortedMap<ResourceType, SortedMap<String, SortedSet<AclEntryKey>>>> _trees;

  /**
   * Create an empty tree.
   */
  public AclTree() {
    _trees = new EnumMap<>(PatternType.class);
    for (PatternType patternType : PATTERN_TYPES) {
      _trees.put(patternType, new TreeMap<>(RESOURCE_TYPE_ORDER));
    }
  }

  /**
   * Set the entries of a resource, replacing any existing ones.
   *
   * @param pattern Resource pattern (LITERAL or PREFIXED).
   * @param entries ACL entries; must not be empty.
   */
  public void put(ResourcePattern pattern, Collection<AclEntryKey> entries) {
    Objects.requireNonNull(pattern, "pattern");
    if (!PATTERN_TYPES.contains(pattern.patternType())) {
      throw new IllegalArgumentException("Unsupported pattern type " + pattern.patternType() + " for " + pattern);
    }
    if (entries == null || entries.isEmpty()) {
      throw new IllegalArgumentException("A resource needs at least one ACL entry: " + pattern);
    }
    _trees.get(pattern.patternType())
          .computeIfAbsent(pattern.resourceType(), t -> new TreeMap<>())
          .put(pattern.name(), new TreeSet<>(entries));
  }

  /**
   * Remove a resource and all its entries.
   *
   * @param pattern Resource pattern.
   * @return {@code true} if the resource was in the tree.
   */
  public boolean remove(ResourcePattern pattern) {
    SortedMap<ResourceType, SortedMap<String, SortedSet<AclEntryKey>>> byType = _trees.get(pattern.patternType());
    SortedMap<String, SortedSet<AclEntryKey>> byName = byType == null ? null : byType.get(pattern.resourceType());
    if (byName == null || byName.remove(pattern.name()) == null) {
      return false;
    }
    if (byName.isEmpty()) {
      byType.remove(pattern.resourceType());
    }
    return true;
  }

  /**
   * @param pattern Resource pattern.
   * @return {@code true} if the resource is in the tree.
   */
  public boolean contains(ResourcePattern pattern) {
    return !entries(pattern).isEmpty();
  }

  /**
   * @param pattern Resource pattern.
   * @return The resource's entries (sorted, unmodifiable), or an empty set if the resource is not in the tree.
   */
  public SortedSet<AclEntryKey> entries(ResourcePattern pattern) {
    SortedMap<ResourceType, SortedMap<String, SortedSet<AclEntryKey>>> byType = _trees.get(pattern.patternType());
    if (byType == null) {
      return Collections.emptySortedSet();
    }
    SortedMap<String, SortedSet<AclEntryKey>> byName = byType.get(pattern.resourceType());
    SortedSet<AclEntryKey> entries = byName == null ? null : byName.get(pattern.name());
    return entries == null ? Collections.emptySortedSet() : Collections.unmodifiableSortedSet(entries);
  }

  /**
   * @param patternType LITERAL or PREFIXED.
   * @return Resource types that have at least one resource, sorted (unmodifiable).
   */
  public SortedSet<ResourceType> resourceTypes(PatternType patternType) {
    SortedSet<ResourceType> types = new TreeSet<>(RESOURCE_TYPE_ORDER);
    types.addAll(_trees.get(patternType).keySet());
    return Collections.unmodifiableSortedSet(types);
  }

  /**
   * @param patternType LITERAL or PREFIXED.
   * @param resourceType Resource type.
   * @return Resource names of that type with their entries, sorted by name (unmodifiable).
   */
  public SortedMap<String, SortedSet<AclEntryKey>> resources(PatternType patternType, ResourceType resourceType) {
    SortedMap<String, SortedSet<AclEntryKey>> byName = _trees.get(patternType).get(resourceType);
    return byName == null ? Collections.emptySortedMap() : Collections.unmodifiableSortedMap(byName);
  }

  /**
   * @return All resource patterns in tree order (LITERAL first, then PREFIXED; by type; by name).
   */
  public List<ResourcePattern> resourcePatterns() {
    List<ResourcePattern> patterns = new ArrayList<>();
    for (PatternType patternType : PATTERN_TYPES) {
      _trees.get(patternType).forEach((type, byName) -> byName.keySet()
          .forEach(name -> patterns.add(new ResourcePattern(type, name, patternType))));
    }
    return patterns;
  }

  /**
   * @return Number of resources (data nodes) in the tree.
   */
  public int numResources() {
    int count = 0;
    for (SortedMap<ResourceType, SortedMap<String, SortedSet<AclEntryKey>>> byType : _trees.values()) {
      for (SortedMap<String, SortedSet<AclEntryKey>> byName : byType.values()) {
        count += byName.size();
      }
    }
    return count;
  }

  /**
   * @return Number of ACL entries in the tree.
   */
  public int numEntries() {
    int count = 0;
    for (SortedMap<ResourceType, SortedMap<String, SortedSet<AclEntryKey>>> byType : _trees.values()) {
      for (SortedMap<String, SortedSet<AclEntryKey>> byName : byType.values()) {
        for (SortedSet<AclEntryKey> entries : byName.values()) {
          count += entries.size();
        }
      }
    }
    return count;
  }

  @Override
  public boolean equals(Object o) {
    return this == o || (o instanceof AclTree && _trees.equals(((AclTree) o)._trees));
  }

  @Override
  public int hashCode() {
    return _trees.hashCode();
  }

  @Override
  public String toString() {
    return String.format("AclTree(resources=%d, entries=%d)", numResources(), numEntries());
  }
}
