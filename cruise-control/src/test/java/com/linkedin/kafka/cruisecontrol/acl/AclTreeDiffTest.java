/*
 * Copyright 2026 qjwu. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.acl;

import com.linkedin.kafka.cruisecontrol.acl.AclTreeDiff.ChangeType;
import com.linkedin.kafka.cruisecontrol.acl.AclTreeDiff.ResourceChange;
import com.linkedin.kafka.cruisecontrol.acl.AclTreeDiff.Result;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AclTreeDiffTest {
  private static final AclEntryKey READ_A = new AclEntryKey("User:a", AclOperation.READ, AclPermissionType.ALLOW, "*");
  private static final AclEntryKey READ_B = new AclEntryKey("User:b", AclOperation.READ, AclPermissionType.ALLOW, "*");
  private static final AclEntryKey DENY_C = new AclEntryKey("Claim:c", AclOperation.READ, AclPermissionType.DENY, "*");
  private static final ResourcePattern T1 = new ResourcePattern(ResourceType.TOPIC, "t1", PatternType.LITERAL);
  private static final ResourcePattern T2 = new ResourcePattern(ResourceType.TOPIC, "t2", PatternType.LITERAL);
  private static final ResourcePattern G1 = new ResourcePattern(ResourceType.GROUP, "g1", PatternType.LITERAL);
  private static final ResourcePattern PG = new ResourcePattern(ResourceType.GROUP, "g-", PatternType.PREFIXED);
  private static final Set<ResourcePattern> NONE = Set.of();

  private static AclTree tree(Object... patternsAndEntries) {
    AclTree tree = new AclTree();
    for (int i = 0; i < patternsAndEntries.length; i += 2) {
      @SuppressWarnings("unchecked")
      List<AclEntryKey> entries = (List<AclEntryKey>) patternsAndEntries[i + 1];
      tree.put((ResourcePattern) patternsAndEntries[i], entries);
    }
    return tree;
  }

  private static AclTree copy(AclTree source) {
    AclTree copy = new AclTree();
    source.resourcePatterns().forEach(p -> copy.put(p, source.entries(p)));
    return copy;
  }

  /**
   * Applies the changes like the writer will (create/rewrite = put, delete = remove).
   *
   * @param current Tree to start from (not modified).
   * @param result Changes to apply.
   * @return The tree after the changes.
   */
  private static AclTree apply(AclTree current, Result result) {
    AclTree applied = copy(current);
    for (ResourceChange change : result.changes()) {
      if (change.type() == ChangeType.DELETED) {
        assertTrue(applied.remove(change.pattern()));
      } else {
        applied.put(change.pattern(), change.after());
      }
    }
    return applied;
  }

  @Test
  public void testIdenticalTreesHaveNoChanges() {
    AclTree a = tree(T1, List.of(READ_A, READ_B), PG, List.of(DENY_C));
    AclTree b = tree(PG, List.of(DENY_C), T1, List.of(READ_B, READ_A));
    Result result = AclTreeDiff.diff(a, b, NONE);
    assertTrue(result.isEmpty());
    assertEquals(0, result.count(ChangeType.UPDATED));
  }

  @Test
  public void testAddedDeletedUpdated() {
    AclTree current = tree(T1, List.of(READ_A, READ_B), T2, List.of(READ_A));
    AclTree desired = tree(T1, List.of(READ_A, DENY_C), G1, List.of(READ_B));

    Result result = AclTreeDiff.diff(current, desired, NONE);

    assertEquals(3, result.changes().size());
    ResourceChange g1 = result.changes().get(0);
    assertEquals(G1, g1.pattern());
    assertEquals(ChangeType.ADDED, g1.type());
    assertEquals(Set.of(READ_B), g1.after());
    assertTrue(g1.before().isEmpty());

    ResourceChange t1 = result.changes().get(1);
    assertEquals(ChangeType.UPDATED, t1.type());
    assertEquals(Set.of(DENY_C), t1.entriesAdded());
    assertEquals(Set.of(READ_B), t1.entriesRemoved());
    assertEquals(Set.of(READ_A, DENY_C), t1.after());

    ResourceChange t2 = result.changes().get(2);
    assertEquals(ChangeType.DELETED, t2.type());
    assertEquals(Set.of(READ_A), t2.entriesRemoved());
    assertTrue(t2.after().isEmpty());

    assertEquals(1, result.count(ChangeType.ADDED));
    assertEquals(1, result.count(ChangeType.DELETED));
    assertEquals(1, result.count(ChangeType.UPDATED));
    assertEquals(desired, apply(current, result));
  }

  @Test
  public void testChangesInTreeOrder() {
    AclTree desired = tree(PG, List.of(READ_A), T2, List.of(READ_A), G1, List.of(READ_A), T1, List.of(READ_A));
    List<ResourcePattern> order = new ArrayList<>();
    AclTreeDiff.diff(new AclTree(), desired, NONE).changes().forEach(c -> order.add(c.pattern()));
    // LITERAL (Group before Topic, t1 before t2), then PREFIXED
    assertEquals(List.of(G1, T1, T2, PG), order);
  }

  @Test
  public void testFirstRunAndEmptyFile() {
    AclTree desired = tree(T1, List.of(READ_A), PG, List.of(READ_B));
    Result firstRun = AclTreeDiff.diff(new AclTree(), desired, NONE);
    assertEquals(2, firstRun.count(ChangeType.ADDED));
    assertEquals(desired, apply(new AclTree(), firstRun));

    // A valid file with no ACLs removes all managed ACLs
    Result emptyFile = AclTreeDiff.diff(desired, new AclTree(), NONE);
    assertEquals(2, emptyFile.count(ChangeType.DELETED));
    assertEquals(new AclTree(), apply(desired, emptyFile));
  }

  @Test
  public void testUnmanagedResourcesAreNeverDeleted() {
    AclTree current = tree(T1, List.of(READ_A), T2, List.of(READ_B));
    AclTree desired = tree(T1, List.of(READ_A));

    Result result = AclTreeDiff.diff(current, desired, Set.of(T2));

    assertTrue(result.isEmpty());
    assertEquals(List.of(T2), result.keptUnmanaged());
  }

  @Test
  public void testFileOverridesUnmanagedResource() {
    AclTree current = tree(T2, List.of(READ_B));
    Result updated = AclTreeDiff.diff(current, tree(T2, List.of(DENY_C)), Set.of(T2));
    assertEquals(1, updated.count(ChangeType.UPDATED));
    assertTrue(updated.keptUnmanaged().isEmpty());

    Result same = AclTreeDiff.diff(current, tree(T2, List.of(READ_B)), Set.of(T2));
    assertTrue(same.isEmpty());
    assertTrue(same.keptUnmanaged().isEmpty());
  }

  @Test
  public void testResourceTypeOnlyOnOneSide() {
    AclTree current = tree(PG, List.of(READ_A));
    AclTree desired = tree(G1, List.of(READ_A));
    Result result = AclTreeDiff.diff(current, desired, NONE);
    assertEquals(2, result.changes().size());
    assertEquals(desired, apply(current, result));
  }

  @Test
  public void testResourcesOnlyIn() {
    AclTree current = tree(T1, List.of(READ_A), T2, List.of(READ_A), PG, List.of(READ_A));
    AclTree desired = tree(T1, List.of(READ_B));
    assertEquals(List.of(T2, PG), AclTreeDiff.resourcesOnlyIn(current, desired));
    assertEquals(List.of(), AclTreeDiff.resourcesOnlyIn(new AclTree(), desired));
  }

  @Test
  public void testRandomTreesApplyToDesired() {
    Random random = new Random(20261114L);
    List<AclEntryKey> pool = List.of(READ_A, READ_B, DENY_C,
                                     new AclEntryKey("User:a", AclOperation.WRITE, AclPermissionType.ALLOW, "*"),
                                     new AclEntryKey("User:a", AclOperation.READ, AclPermissionType.ALLOW, "10.0.0.1"));
    List<ResourcePattern> resources = new ArrayList<>();
    for (PatternType pt : AclTree.PATTERN_TYPES) {
      for (ResourceType type : List.of(ResourceType.TOPIC, ResourceType.GROUP, ResourceType.TRANSACTIONAL_ID)) {
        for (String name : List.of("a", "B", "c-", "orders")) {
          resources.add(new ResourcePattern(type, name, pt));
        }
      }
    }
    for (int round = 0; round < 200; round++) {
      AclTree current = randomTree(random, resources, pool);
      AclTree desired = randomTree(random, resources, pool);
      Set<ResourcePattern> unmanaged = new HashSet<>();
      for (ResourcePattern p : resources) {
        if (random.nextInt(5) == 0) {
          unmanaged.add(p);
        }
      }
      Result result = AclTreeDiff.diff(current, desired, unmanaged);
      AclTree expected = copy(desired);
      result.keptUnmanaged().forEach(p -> expected.put(p, current.entries(p)));
      assertEquals("round " + round, expected, apply(current, result));
      // Re-running on the result finds nothing more to do
      assertTrue("round " + round, AclTreeDiff.diff(apply(current, result), desired, unmanaged).isEmpty());
    }
  }

  private static AclTree randomTree(Random random, List<ResourcePattern> resources, List<AclEntryKey> pool) {
    AclTree tree = new AclTree();
    for (ResourcePattern p : resources) {
      if (random.nextBoolean()) {
        List<AclEntryKey> entries = new ArrayList<>();
        pool.forEach(e -> {
          if (random.nextBoolean()) {
            entries.add(e);
          }
        });
        if (!entries.isEmpty()) {
          tree.put(p, entries);
        }
      }
    }
    return tree;
  }
}
