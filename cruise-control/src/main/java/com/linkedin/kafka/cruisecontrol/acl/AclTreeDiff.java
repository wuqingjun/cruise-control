/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.acl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;

/**
 * Compares two {@link AclTree}s node by node and lists every resource that must change to turn the current tree
 * (what is in ZooKeeper) into the desired tree (the file).
 * <p>
 * Both trees are sorted at every level, so each level is compared by walking two sorted sequences side by side
 * (a merge): resource types, then resource names, then entries. A resource only in the desired tree is ADDED, only in
 * the current tree is DELETED, in both with different entries is UPDATED (with the entries added and removed).
 * Changes are returned in tree order.
 * <p>
 * Unmanaged resources (ACLs that existed before Cruise Control's first apply and are not in the file) are never deleted;
 * they are listed in {@link Result#keptUnmanaged()}. If the file defines an unmanaged resource, the file overrides it
 * like any other resource.
 */
public final class AclTreeDiff {

  private AclTreeDiff() {
  }

  /** Kind of change of one resource. */
  public enum ChangeType {
    /** Only in the desired tree: create the data node. */
    ADDED,
    /** Only in the current tree: delete the data node. */
    DELETED,
    /** In both, with different entries: rewrite the data node. */
    UPDATED
  }

  /** The change of one resource. */
  public static final class ResourceChange {
    private final ResourcePattern _pattern;
    private final ChangeType _type;
    private final SortedSet<AclEntryKey> _before;
    private final SortedSet<AclEntryKey> _after;

    ResourceChange(ResourcePattern pattern, ChangeType type, SortedSet<AclEntryKey> before, SortedSet<AclEntryKey> after) {
      _pattern = pattern;
      _type = type;
      _before = Collections.unmodifiableSortedSet(new TreeSet<>(before));
      _after = Collections.unmodifiableSortedSet(new TreeSet<>(after));
    }

    public ResourcePattern pattern() {
      return _pattern;
    }

    public ChangeType type() {
      return _type;
    }

    /**
     * @return Entries currently in ZooKeeper (empty if ADDED).
     */
    public SortedSet<AclEntryKey> before() {
      return _before;
    }

    /**
     * @return Entries after the change (empty if DELETED): the data node's new content.
     */
    public SortedSet<AclEntryKey> after() {
      return _after;
    }

    /**
     * @return Entries in {@link #after()} but not in {@link #before()}.
     */
    public SortedSet<AclEntryKey> entriesAdded() {
      return minus(_after, _before);
    }

    /**
     * @return Entries in {@link #before()} but not in {@link #after()}.
     */
    public SortedSet<AclEntryKey> entriesRemoved() {
      return minus(_before, _after);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof ResourceChange)) {
        return false;
      }
      ResourceChange that = (ResourceChange) o;
      return _pattern.equals(that._pattern) && _type == that._type && _before.equals(that._before) && _after.equals(that._after);
    }

    @Override
    public int hashCode() {
      return Objects.hash(_pattern, _type, _before, _after);
    }

    @Override
    public String toString() {
      return String.format("%s %s (+%d -%d entries)", _type, AclTreeParser.location(_pattern), entriesAdded().size(),
                           entriesRemoved().size());
    }
  }

  /** All changes, plus the unmanaged resources that were kept. */
  public static final class Result {
    private final List<ResourceChange> _changes;
    private final List<ResourcePattern> _keptUnmanaged;

    Result(List<ResourceChange> changes, List<ResourcePattern> keptUnmanaged) {
      _changes = Collections.unmodifiableList(changes);
      _keptUnmanaged = Collections.unmodifiableList(keptUnmanaged);
    }

    /**
     * @return Changes in tree order (LITERAL before PREFIXED, types by folder name, names alphabetically).
     */
    public List<ResourceChange> changes() {
      return _changes;
    }

    /**
     * @return Unmanaged resources present in ZooKeeper but not in the file, left untouched (in tree order).
     */
    public List<ResourcePattern> keptUnmanaged() {
      return _keptUnmanaged;
    }

    /**
     * @return {@code true} if ZooKeeper already matches the file.
     */
    public boolean isEmpty() {
      return _changes.isEmpty();
    }

    /**
     * @param type Change type.
     * @return Number of changes of that type.
     */
    public int count(ChangeType type) {
      return (int) _changes.stream().filter(c -> c.type() == type).count();
    }

    /**
     * @return Counts per change type, e.g. {@code {ADDED=2, DELETED=0, UPDATED=1}}.
     */
    public Map<ChangeType, Integer> counts() {
      Map<ChangeType, Integer> counts = new EnumMap<>(ChangeType.class);
      for (ChangeType type : ChangeType.values()) {
        counts.put(type, count(type));
      }
      return counts;
    }

    @Override
    public String toString() {
      return String.format("AclTreeDiff(added=%d, deleted=%d, updated=%d, keptUnmanaged=%d)", count(ChangeType.ADDED),
                           count(ChangeType.DELETED), count(ChangeType.UPDATED), _keptUnmanaged.size());
    }
  }

  /**
   * Compare two trees.
   *
   * @param current What is in ZooKeeper.
   * @param desired What the file says.
   * @param unmanaged Resources never to delete (pre-existing at the first apply); may be empty.
   * @return The changes that turn {@code current} into {@code desired}, except deleting unmanaged resources.
   */
  public static Result diff(AclTree current, AclTree desired, Set<ResourcePattern> unmanaged) {
    Objects.requireNonNull(current, "current");
    Objects.requireNonNull(desired, "desired");
    Objects.requireNonNull(unmanaged, "unmanaged");
    List<ResourceChange> changes = new ArrayList<>();
    List<ResourcePattern> keptUnmanaged = new ArrayList<>();
    for (PatternType patternType : AclTree.PATTERN_TYPES) {
      SortedSet<ResourceType> types = new TreeSet<>(AclTree.RESOURCE_TYPE_ORDER);
      types.addAll(current.resourceTypes(patternType));
      types.addAll(desired.resourceTypes(patternType));
      for (ResourceType type : types) {
        mergeResources(patternType, type, current.resources(patternType, type), desired.resources(patternType, type),
                       unmanaged, changes, keptUnmanaged);
      }
    }
    return new Result(changes, keptUnmanaged);
  }

  /**
   * @param current What is in ZooKeeper.
   * @param desired What the file says.
   * @return Resources in {@code current} but not in {@code desired}, in tree order (used to find the unmanaged
   * resources at the first apply).
   */
  public static List<ResourcePattern> resourcesOnlyIn(AclTree current, AclTree desired) {
    List<ResourcePattern> only = new ArrayList<>();
    for (ResourcePattern pattern : current.resourcePatterns()) {
      if (!desired.contains(pattern)) {
        only.add(pattern);
      }
    }
    return only;
  }

  private static void mergeResources(PatternType patternType, ResourceType type,
                                     SortedMap<String, SortedSet<AclEntryKey>> current,
                                     SortedMap<String, SortedSet<AclEntryKey>> desired, Set<ResourcePattern> unmanaged,
                                     List<ResourceChange> changes, List<ResourcePattern> keptUnmanaged) {
    Iterator<Map.Entry<String, SortedSet<AclEntryKey>>> c = current.entrySet().iterator();
    Iterator<Map.Entry<String, SortedSet<AclEntryKey>>> d = desired.entrySet().iterator();
    Map.Entry<String, SortedSet<AclEntryKey>> cur = next(c);
    Map.Entry<String, SortedSet<AclEntryKey>> des = next(d);
    while (cur != null || des != null) {
      int cmp = cur == null ? 1 : des == null ? -1 : cur.getKey().compareTo(des.getKey());
      if (cmp < 0) {
        ResourcePattern pattern = new ResourcePattern(type, cur.getKey(), patternType);
        if (unmanaged.contains(pattern)) {
          keptUnmanaged.add(pattern);
        } else {
          changes.add(new ResourceChange(pattern, ChangeType.DELETED, cur.getValue(), Collections.emptySortedSet()));
        }
        cur = next(c);
      } else if (cmp > 0) {
        ResourcePattern pattern = new ResourcePattern(type, des.getKey(), patternType);
        changes.add(new ResourceChange(pattern, ChangeType.ADDED, Collections.emptySortedSet(), des.getValue()));
        des = next(d);
      } else {
        if (!sameEntries(cur.getValue(), des.getValue())) {
          ResourcePattern pattern = new ResourcePattern(type, cur.getKey(), patternType);
          changes.add(new ResourceChange(pattern, ChangeType.UPDATED, cur.getValue(), des.getValue()));
        }
        cur = next(c);
        des = next(d);
      }
    }
  }

  /**
   * Compares two sorted entry sets element by element (both are in {@link AclEntryKey} order).
   *
   * @param a First set.
   * @param b Second set.
   * @return {@code true} if both contain the same entries.
   */
  private static boolean sameEntries(SortedSet<AclEntryKey> a, SortedSet<AclEntryKey> b) {
    if (a.size() != b.size()) {
      return false;
    }
    Iterator<AclEntryKey> x = a.iterator();
    Iterator<AclEntryKey> y = b.iterator();
    while (x.hasNext()) {
      if (x.next().compareTo(y.next()) != 0) {
        return false;
      }
    }
    return true;
  }

  private static SortedSet<AclEntryKey> minus(SortedSet<AclEntryKey> a, SortedSet<AclEntryKey> b) {
    SortedSet<AclEntryKey> result = new TreeSet<>(a);
    result.removeAll(b);
    return Collections.unmodifiableSortedSet(result);
  }

  private static <T> T next(Iterator<T> it) {
    return it.hasNext() ? it.next() : null;
  }
}
