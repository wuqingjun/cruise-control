/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.quota;

import com.linkedin.kafka.cruisecontrol.acl.AclFileException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class QuotaFileParserTest {
  private static final List<String> TYPES = List.of("User", "Claim", "Certificate");
  static final String SAMPLE = "{\n"
      + "  \"version\": 1,\n"
      + "  \"default\": {\"producer_byte_rate\": 1024, \"consumer_byte_rate\": 2048, \"request_percentage\": 5},\n"
      + "  \"quotas\": {\n"
      + "    \"Claim:payments-team\": {\"producer_byte_rate\": 50000, \"consumer_byte_rate\": 100000},\n"
      + "    \"User:O=LocalDev,CN=cruise-control\": {\"producer_byte_rate\": 1e8, \"request_percentage\": 100}\n"
      + "  }\n"
      + "}";

  private static QuotaSet parse(String json) throws AclFileException {
    return QuotaFileParser.parse(json.getBytes(StandardCharsets.UTF_8), TYPES);
  }

  private static String problem(String json) {
    return assertThrows(AclFileException.class, () -> parse(json)).getMessage();
  }

  @Test
  public void testSample() throws AclFileException {
    QuotaSet quotas = parse(SAMPLE);
    assertEquals(3, quotas.size());
    assertEquals(Map.of("producer_byte_rate", 1024.0, "consumer_byte_rate", 2048.0, "request_percentage", 5.0),
                 quotas.get(QuotaSet.DEFAULT));
    assertEquals(Map.of("producer_byte_rate", 50000.0, "consumer_byte_rate", 100000.0), quotas.get("Claim:payments-team"));
    assertEquals(Map.of("producer_byte_rate", 1e8, "request_percentage", 100.0), quotas.get("User:O=LocalDev,CN=cruise-control"));
  }

  @Test
  public void testDefaultAndVersionRequired() {
    assertTrue(problem("{\"version\": 1, \"quotas\": {}}").contains("Missing or empty 'default'"));
    assertTrue(problem("{\"version\": 1, \"default\": {}}").contains("Missing or empty 'default'"));
    assertTrue(problem("{\"default\": {\"producer_byte_rate\": 1}}").contains("Missing 'version'"));
    assertTrue(problem("{\"version\": 2, \"default\": {\"producer_byte_rate\": 1}}").contains("Unsupported quota file version"));
  }

  @Test
  public void testAllProblemsReported() {
    String message = problem("{\"version\": 1, \"default\": {\"producer_byte_rate\": 0}, \"quotas\": {"
                             + "\"Group:admins\": {\"producer_byte_rate\": 10},"
                             + "\"no-type\": {\"consumer_byte_rate\": 10},"
                             + "\"Claim:x\": {\"bytes_per_week\": 10, \"request_percentage\": -1},"
                             + "\"Claim:empty\": {}}}");
    assertTrue(message, message.contains("default/producer_byte_rate: must be a positive number"));
    assertTrue(message, message.contains("quotas/Group:admins: identity type 'Group' is not allowed"));
    assertTrue(message, message.contains("quotas/no-type: identity must be 'Type:name'"));
    assertTrue(message, message.contains("unknown quota 'bytes_per_week'"));
    assertTrue(message, message.contains("quotas/Claim:x/request_percentage: must be a positive number"));
    assertTrue(message, message.contains("quotas/Claim:empty: no quota values"));
  }

  @Test
  public void testStructureErrors() {
    assertTrue(problem("").contains("empty"));
    assertTrue(problem("not json").contains("Invalid JSON"));
    assertTrue(problem("{\"version\": 1, \"version\": 1}").contains("Duplicate top-level key"));
    assertTrue(problem("{\"version\": 1, \"extra\": 1}").contains("Unknown top-level key 'extra'"));
    assertTrue(problem("{\"version\": 1, \"default\": {\"producer_byte_rate\": 1, \"producer_byte_rate\": 2}}")
                   .contains("Duplicate key 'producer_byte_rate'"));
    assertTrue(problem("{\"version\": 1, \"default\": {\"producer_byte_rate\": \"fast\"}}").contains("expected NUMBER"));
    assertTrue(problem(SAMPLE + " {}").contains("Unexpected content after the top-level object"));
  }

  @Test
  public void testDiff() throws AclFileException {
    QuotaSet desired = parse(SAMPLE);
    QuotaSet stored = new QuotaSet(Map.of(
        QuotaSet.DEFAULT, Map.of("producer_byte_rate", 1024.0, "consumer_byte_rate", 2048.0, "request_percentage", 5.0),
        "Claim:payments-team", Map.of("producer_byte_rate", 1.0, "request_percentage", 7.0),
        "alice", Map.of("producer_byte_rate", 10.0)));
    List<QuotaDiff.Change> changes = QuotaDiff.diff(stored, desired);
    assertEquals(3, changes.size());
    // identity order: Claim:..., User:..., alice
    assertEquals("UPDATED Claim:payments-team: set {consumer_byte_rate=100000.0, producer_byte_rate=50000.0}, "
                 + "remove [request_percentage]", changes.get(0).toString());
    assertEquals(QuotaDiff.Change.Type.ADDED, changes.get(1).type());
    assertEquals("User:O=LocalDev,CN=cruise-control", changes.get(1).identity());
    assertEquals("REMOVED alice: set {}, remove [producer_byte_rate]", changes.get(2).toString());
    assertEquals(List.of(), QuotaDiff.diff(desired, desired));
  }
}
