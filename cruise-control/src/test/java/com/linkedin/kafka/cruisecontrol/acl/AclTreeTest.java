/*
 * Copyright 2026 qjwu. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.acl;

import java.util.List;
import java.util.TreeSet;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class AclTreeTest {
  private static final AclEntryKey A = new AclEntryKey("User:a", AclOperation.READ, AclPermissionType.ALLOW, "*");
  private static final AclEntryKey B = new AclEntryKey("User:b", AclOperation.READ, AclPermissionType.ALLOW, "*");

  private static ResourcePattern literal(ResourceType type, String name) {
    return new ResourcePattern(type, name, PatternType.LITERAL);
  }

  @Test
  public void testOrderLiteralFirstThenTypesByFolderNameThenNames() {
    AclTree tree = new AclTree();
    tree.put(new ResourcePattern(ResourceType.GROUP, "g-", PatternType.PREFIXED), List.of(A));
    tree.put(literal(ResourceType.USER, "u"), List.of(A));
    tree.put(literal(ResourceType.TOPIC, "b"), List.of(A));
    tree.put(literal(ResourceType.TOPIC, "B"), List.of(A));
    tree.put(literal(ResourceType.TOPIC, "a"), List.of(A));
    tree.put(literal(ResourceType.TRANSACTIONAL_ID, "tx"), List.of(A));
    tree.put(literal(ResourceType.DELEGATION_TOKEN, "d"), List.of(A));
    tree.put(literal(ResourceType.CLUSTER, "kafka-cluster"), List.of(A));

    assertEquals(List.of(literal(ResourceType.CLUSTER, "kafka-cluster"), literal(ResourceType.DELEGATION_TOKEN, "d"),
                         literal(ResourceType.TOPIC, "B"), literal(ResourceType.TOPIC, "a"), literal(ResourceType.TOPIC, "b"),
                         literal(ResourceType.TRANSACTIONAL_ID, "tx"), literal(ResourceType.USER, "u"),
                         new ResourcePattern(ResourceType.GROUP, "g-", PatternType.PREFIXED)),
                 tree.resourcePatterns());
    assertEquals(8, tree.numResources());
  }

  @Test
  public void testEntriesSortedAndReplaced() {
    AclTree tree = new AclTree();
    ResourcePattern topic = literal(ResourceType.TOPIC, "t");
    tree.put(topic, List.of(B, A));
    assertEquals(List.of(A, B), List.copyOf(tree.entries(topic)));
    tree.put(topic, List.of(B));
    assertEquals(List.of(B), List.copyOf(tree.entries(topic)));
    assertEquals(1, tree.numEntries());
    assertTrue(tree.entries(literal(ResourceType.TOPIC, "missing")).isEmpty());
  }

  @Test
  public void testInvalidPutsRejected() {
    AclTree tree = new AclTree();
    assertThrows(IllegalArgumentException.class, () -> tree.put(literal(ResourceType.TOPIC, "t"), List.of()));
    assertThrows(IllegalArgumentException.class,
                 () -> tree.put(new ResourcePattern(ResourceType.TOPIC, "t", PatternType.MATCH), List.of(A)));
  }

  @Test
  public void testEquality() {
    AclTree x = new AclTree();
    AclTree y = new AclTree();
    x.put(literal(ResourceType.TOPIC, "t"), List.of(A, B));
    y.put(literal(ResourceType.TOPIC, "t"), List.of(B, A));
    assertEquals(x, y);
    assertEquals(x.hashCode(), y.hashCode());
    y.put(literal(ResourceType.TOPIC, "u"), List.of(A));
    assertNotEquals(x, y);
  }

  @Test
  public void testEntryKeyOrderAndEquality() {
    AclEntryKey deny = new AclEntryKey("User:a", AclOperation.READ, AclPermissionType.DENY, "*");
    AclEntryKey write = new AclEntryKey("User:a", AclOperation.WRITE, AclPermissionType.ALLOW, "*");
    AclEntryKey host = new AclEntryKey("User:a", AclOperation.READ, AclPermissionType.ALLOW, "10.0.0.1");
    // principal, then operation name (Read < Write), then permission name (Allow < Deny), then host ("*" < "10...")
    assertEquals(List.of(A, host, deny, write, B), List.copyOf(new TreeSet<>(List.of(B, write, deny, host, A))));
    assertEquals(A, AclEntryKey.of(A.toAccessControlEntry()));
    assertEquals("User:a Allow Read from *", A.toString());
  }
}
