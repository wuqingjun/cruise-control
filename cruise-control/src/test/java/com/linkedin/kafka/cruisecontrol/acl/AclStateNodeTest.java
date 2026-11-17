/*
 * Copyright 2026 qjwu. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.acl;

import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class AclStateNodeTest {

  @Test
  public void testRoundTrip() {
    AclStateNode state = new AclStateNode("ab12", 1794620000000L, "/opt/kafka-acls/acls.json",
                                          List.of(new ResourcePattern(ResourceType.GROUP, "g-", PatternType.PREFIXED),
                                                  new ResourcePattern(ResourceType.TOPIC, "legacy", PatternType.LITERAL)));
    AclStateNode copy = AclStateNode.fromBytes(state.toBytes());
    assertEquals(state, copy);
    // tree order: literal before prefixed
    assertEquals(List.of(new ResourcePattern(ResourceType.TOPIC, "legacy", PatternType.LITERAL),
                         new ResourcePattern(ResourceType.GROUP, "g-", PatternType.PREFIXED)), List.copyOf(copy.unmanaged()));
  }

  @Test
  public void testInvalidContentRejected() {
    assertThrows(IllegalArgumentException.class, () -> AclStateNode.fromBytes("not json".getBytes(StandardCharsets.UTF_8)));
    assertThrows(IllegalArgumentException.class, () -> AclStateNode.fromBytes(
        "{\"version\":2,\"sha256\":\"x\",\"appliedAtMs\":1,\"file\":\"f\",\"unmanaged\":[]}".getBytes(StandardCharsets.UTF_8)));
  }
}
