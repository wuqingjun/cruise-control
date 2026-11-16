/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.acl;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class KafkaAclZkCodecTest {
  private static final ResourcePattern TOPIC = new ResourcePattern(ResourceType.TOPIC, "test-authz", PatternType.LITERAL);
  private static final ResourcePattern GROUP_PREFIX = new ResourcePattern(ResourceType.GROUP, "saml-test-", PatternType.PREFIXED);
  private static final AclEntryKey READERS = new AclEntryKey("Claim:kafka-readers", AclOperation.READ, AclPermissionType.ALLOW, "*");
  private static final AclEntryKey WRITERS = new AclEntryKey("Claim:kafka-writers", AclOperation.WRITE, AclPermissionType.ALLOW, "*");
  private static final AclEntryKey CONTRACTORS = new AclEntryKey("Claim:contractors", AclOperation.READ, AclPermissionType.DENY, "*");

  @Test
  public void testPathsMatchKafkasZooKeeperLayout() {
    assertEquals("/kafka-acl", KafkaAclZkCodec.rootPath(PatternType.LITERAL));
    assertEquals("/kafka-acl-extended/prefixed", KafkaAclZkCodec.rootPath(PatternType.PREFIXED));
    assertEquals("/kafka-acl/Topic", KafkaAclZkCodec.typePath(PatternType.LITERAL, ResourceType.TOPIC));
    assertEquals("/kafka-acl/TransactionalId", KafkaAclZkCodec.typePath(PatternType.LITERAL, ResourceType.TRANSACTIONAL_ID));
    assertEquals("/kafka-acl/Topic/test-authz", KafkaAclZkCodec.dataPath(TOPIC));
    assertEquals("/kafka-acl-extended/prefixed/Group/saml-test-", KafkaAclZkCodec.dataPath(GROUP_PREFIX));
    assertEquals("/kafka-acl-changes", KafkaAclZkCodec.changeRootPath(PatternType.LITERAL));
    assertEquals("/kafka-acl-extended-changes", KafkaAclZkCodec.changeRootPath(PatternType.PREFIXED));
  }

  @Test
  public void testFileKeysMatchZooKeeperFolders() {
    assertEquals("/" + AclTreeParser.LITERAL_TREE, KafkaAclZkCodec.rootPath(PatternType.LITERAL));
    assertEquals("/" + AclTreeParser.EXTENDED_TREE + "/" + AclTreeParser.PREFIXED_FOLDER,
                 KafkaAclZkCodec.rootPath(PatternType.PREFIXED));
    assertEquals(KafkaAclZkCodec.dataPath(TOPIC), "/" + AclTreeParser.location(TOPIC));
    assertEquals(KafkaAclZkCodec.dataPath(GROUP_PREFIX), "/" + AclTreeParser.location(GROUP_PREFIX));
  }

  @Test
  public void testChangeNodesAreDecodedByKafka() {
    KafkaAclZkCodec.ChangeNode literal = KafkaAclZkCodec.changeNode(TOPIC);
    assertEquals("/kafka-acl-changes/acl_changes_", literal.pathPrefix());
    assertEquals(TOPIC, KafkaAclZkCodec.decodeChangeNode(PatternType.LITERAL, literal.data()));

    KafkaAclZkCodec.ChangeNode prefixed = KafkaAclZkCodec.changeNode(GROUP_PREFIX);
    assertEquals("/kafka-acl-extended-changes/acl_changes_", prefixed.pathPrefix());
    JsonObject event = JsonParser.parseString(new String(prefixed.data(), StandardCharsets.UTF_8)).getAsJsonObject();
    assertEquals("PREFIXED", event.get("patternType").getAsString());
    assertEquals(GROUP_PREFIX, KafkaAclZkCodec.decodeChangeNode(PatternType.PREFIXED, prefixed.data()));
  }

  @Test
  public void testEncodeDecodeRoundTripAndSortedOutput() {
    SortedSet<AclEntryKey> entries = new TreeSet<>(List.of(WRITERS, READERS, CONTRACTORS));
    byte[] bytes = KafkaAclZkCodec.encode(List.of(WRITERS, CONTRACTORS, READERS));

    assertEquals(entries, KafkaAclZkCodec.decode(bytes));
    JsonObject node = JsonParser.parseString(new String(bytes, StandardCharsets.UTF_8)).getAsJsonObject();
    assertEquals(1, node.get("version").getAsInt());
    JsonArray acls = node.getAsJsonArray("acls");
    assertEquals("Claim:contractors", acls.get(0).getAsJsonObject().get("principal").getAsString());
    assertEquals("Claim:kafka-readers", acls.get(1).getAsJsonObject().get("principal").getAsString());
    assertEquals("Claim:kafka-writers", acls.get(2).getAsJsonObject().get("principal").getAsString());
    assertEquals("Deny", acls.get(0).getAsJsonObject().get("permissionType").getAsString());
    assertEquals("Read", acls.get(0).getAsJsonObject().get("operation").getAsString());
    // Same input in any order gives the same bytes
    assertArrayEquals(bytes, KafkaAclZkCodec.encode(List.of(READERS, CONTRACTORS, WRITERS)));
  }

  @Test
  public void testDecodesWhatKafka39BrokersWrite() {
    // Exactly as read from the local cluster's /kafka/kafka-acl/Topic/test-authz (Kafka 3.9)
    String fromZooKeeper = "{\"acls\":[{\"principal\":\"Claim:kafka-writers\",\"permissionType\":\"Allow\",\"host\":\"*\","
        + "\"operation\":\"Write\"},{\"principal\":\"Claim:kafka-readers\",\"permissionType\":\"Allow\",\"host\":\"*\","
        + "\"operation\":\"Read\"},{\"principal\":\"Claim:contractors\",\"permissionType\":\"Deny\",\"host\":\"*\","
        + "\"operation\":\"Read\"}],\"version\":1}";
    assertEquals(new TreeSet<>(List.of(WRITERS, READERS, CONTRACTORS)),
                 KafkaAclZkCodec.decode(fromZooKeeper.getBytes(StandardCharsets.UTF_8)));
  }

  @Test
  public void testDecodeRejectsInvalidData() {
    assertThrows(IllegalArgumentException.class,
                 () -> KafkaAclZkCodec.decode("{\"version\":2,\"acls\":[]}".getBytes(StandardCharsets.UTF_8)));
    assertThrows(IllegalArgumentException.class, () -> KafkaAclZkCodec.decode(
        "{\"version\":1,\"acls\":[{\"principal\":\"alice\",\"permissionType\":\"Allow\",\"host\":\"*\",\"operation\":\"Read\"}]}"
            .getBytes(StandardCharsets.UTF_8)));
  }

  @Test
  public void testSupportedOperationsAndResourceTypes() {
    assertTrue(KafkaAclZkCodec.supportedOperations(ResourceType.TOPIC).contains(AclOperation.READ));
    assertFalse(KafkaAclZkCodec.supportedOperations(ResourceType.TOPIC).contains(AclOperation.CLUSTER_ACTION));
    assertTrue(KafkaAclZkCodec.supportedOperations(ResourceType.CLUSTER).contains(AclOperation.CLUSTER_ACTION));
    assertEquals(6, KafkaAclZkCodec.resourceTypes().size());
    assertFalse(KafkaAclZkCodec.resourceTypes().contains(ResourceType.ANY));
  }

  @Test
  public void testUnsupportedPatternTypeRejected() {
    assertThrows(IllegalArgumentException.class, () -> KafkaAclZkCodec.rootPath(PatternType.MATCH));
  }
}
