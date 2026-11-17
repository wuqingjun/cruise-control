/*
 * Copyright 2026 qjwu. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.acl;

import java.util.List;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.acl.AclTestFiles.bytes;
import static com.linkedin.kafka.cruisecontrol.acl.AclTestFiles.entry;
import static com.linkedin.kafka.cruisecontrol.acl.AclTestFiles.oneTopic;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class AclTreeParserTest {
  private static final String READ = entry("User:alice", "Allow", "*", "Read");

  private static String rejected(String json) {
    AclFileException e = assertThrows(AclFileException.class, () -> AclTreeParser.parse(bytes(json)));
    return e.getMessage();
  }

  @Test
  public void testParsesLocalClusterFile() throws AclFileException {
    AclTree tree = AclTreeParser.parse(bytes(AclTestFiles.LOCAL_CLUSTER));

    assertEquals(2, tree.numResources());
    assertEquals(4, tree.numEntries());
    ResourcePattern topic = new ResourcePattern(ResourceType.TOPIC, "test-authz", PatternType.LITERAL);
    assertEquals(3, tree.entries(topic).size());
    assertTrue(tree.entries(topic).contains(
        new AclEntryKey("Claim:contractors", AclOperation.READ, AclPermissionType.DENY, "*")));
    ResourcePattern group = new ResourcePattern(ResourceType.GROUP, "saml-test-", PatternType.PREFIXED);
    assertEquals(List.of(new AclEntryKey("Claim:kafka-readers", AclOperation.READ, AclPermissionType.ALLOW, "*")),
                 List.copyOf(tree.entries(group)));
  }

  @Test
  public void testAllSixResourceTypeFolders() throws AclFileException {
    String json = "{\"kafka-acl\": {"
        + "\"Cluster\": {\"kafka-cluster\": {\"version\": 1, \"acls\": [" + entry("User:a", "Allow", "*", "Describe") + "]}},"
        + "\"DelegationToken\": {\"t1\": {\"version\": 1, \"acls\": [" + entry("User:a", "Allow", "*", "Describe") + "]}},"
        + "\"Group\": {\"g1\": {\"version\": 1, \"acls\": [" + READ + "]}},"
        + "\"Topic\": {\"t1\": {\"version\": 1, \"acls\": [" + READ + "]}},"
        + "\"TransactionalId\": {\"tx1\": {\"version\": 1, \"acls\": [" + entry("User:a", "Allow", "*", "Write") + "]}},"
        + "\"User\": {\"u1\": {\"version\": 1, \"acls\": [" + entry("User:a", "Allow", "*", "CreateTokens") + "]}}"
        + "}}";
    AclTree tree = AclTreeParser.parse(bytes(json));
    assertEquals(6, tree.numResources());
    assertEquals(6, tree.resourceTypes(PatternType.LITERAL).size());
  }

  @Test
  public void testEmptyStructuresAreValid() throws AclFileException {
    assertEquals(0, AclTreeParser.parse(bytes("{}")).numResources());
    assertEquals(0, AclTreeParser.parse(bytes("{\"kafka-acl\": {}, \"kafka-acl-extended\": {\"prefixed\": {}}}")).numResources());
    assertEquals(0, AclTreeParser.parse(bytes("{\"kafka-acl\": {\"Topic\": {}}}")).numResources());
  }

  @Test
  public void testLocation() {
    assertEquals("kafka-acl/Topic/orders",
                 AclTreeParser.location(new ResourcePattern(ResourceType.TOPIC, "orders", PatternType.LITERAL)));
    assertEquals("kafka-acl-extended/prefixed/Group/orders-",
                 AclTreeParser.location(new ResourcePattern(ResourceType.GROUP, "orders-", PatternType.PREFIXED)));
  }

  @Test
  public void testBrokenFilesRejected() {
    assertTrue(assertThrows(AclFileException.class, () -> AclTreeParser.parse(new byte[0])).getMessage().contains("empty"));
    assertTrue(assertThrows(AclFileException.class, () -> AclTreeParser.parse(bytes("  \n"))).getMessage().contains("empty"));
    assertTrue(rejected("{\"kafka-acl\": ").contains("Invalid JSON"));
    assertTrue(rejected("[]").contains("expected a JSON object"));
    assertTrue(rejected("{} {}").contains("after the top-level object"));
  }

  @Test
  public void testUnknownKeysRejected() {
    assertTrue(rejected("{\"kafka-acls\": {}}").contains("Unknown top-level key 'kafka-acls'"));
    assertTrue(rejected("{\"kafka-acl-extended\": {\"literal\": {}}}").contains("unknown folder 'literal'"));
    assertTrue(rejected("{\"kafka-acl\": {\"topic\": {}}}").contains("unknown resource type 'topic'"));
    assertTrue(rejected("{\"kafka-acl\": {\"Topics\": {}}}").contains("unknown resource type 'Topics'"));
    assertTrue(rejected("{\"kafka-acl\": {\"Any\": {}}}").contains("unknown resource type 'Any'"));
  }

  @Test
  public void testDuplicateKeysRejected() {
    assertTrue(rejected("{\"kafka-acl\": {}, \"kafka-acl\": {}}").contains("duplicate key 'kafka-acl'"));
    String leaf = "{\"version\": 1, \"acls\": [" + READ + "]}";
    assertTrue(rejected("{\"kafka-acl\": {\"Topic\": {\"t\": " + leaf + ", \"t\": " + leaf + "}}}")
                   .contains("kafka-acl/Topic: duplicate key 't'"));
  }

  @Test
  public void testInvalidLeavesRejected() {
    assertTrue(rejected("{\"kafka-acl\": {\"Topic\": {\"t\": []}}}").contains("kafka-acl/Topic/t: expected"));
    assertTrue(rejected("{\"kafka-acl\": {\"Topic\": {\"t\": {\"version\": 2, \"acls\": [" + READ + "]}}}}")
                   .contains("'version' must be 1"));
    assertTrue(rejected("{\"kafka-acl\": {\"Topic\": {\"t\": {\"version\": 1}}}}").contains("'acls' must be a list"));
    assertTrue(rejected(oneTopic("")).contains("no ACL entries"));
    assertTrue(rejected(oneTopic(READ + "," + READ)).contains("1 duplicate ACL entry"));
    assertTrue(rejected(oneTopic(entry("alice", "Allow", "*", "Read"))).contains("kafka-acl/Topic/t: invalid ACL entry"));
  }
}
