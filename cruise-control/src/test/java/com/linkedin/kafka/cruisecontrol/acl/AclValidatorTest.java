/*
 * Copyright 2026 qjwu. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.acl;

import java.util.List;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.acl.AclTestFiles.bytes;
import static com.linkedin.kafka.cruisecontrol.acl.AclTestFiles.entry;
import static com.linkedin.kafka.cruisecontrol.acl.AclTestFiles.oneTopic;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class AclValidatorTest {
  private static final List<String> TYPES = List.of("User", "Claim", "Certificate");

  private static List<String> problems(String json) throws AclFileException {
    return AclValidator.validate(AclTreeParser.parse(bytes(json)), TYPES);
  }

  private static void assertOneProblem(String json, String expected) throws AclFileException {
    List<String> problems = problems(json);
    assertEquals(problems.toString(), 1, problems.size());
    assertTrue(problems.get(0), problems.get(0).contains(expected));
  }

  @Test
  public void testLocalClusterFileIsValid() throws AclFileException {
    assertEquals(List.of(), problems(AclTestFiles.LOCAL_CLUSTER));
  }

  @Test
  public void testValidEntries() throws AclFileException {
    assertEquals(List.of(), problems(oneTopic(entry("User:*", "Allow", "*", "All"))));
    assertEquals(List.of(), problems(oneTopic(entry("Certificate:spiffe://example.org/sa/a", "Deny", "10.0.0.1", "Write"))));
    assertEquals(List.of(), problems(oneTopic(entry("Claim:urn:example:kafka:ops", "Allow", "::1", "DescribeConfigs"))));
  }

  @Test
  public void testPrincipalTypeNotAllowed() throws AclFileException {
    assertOneProblem(oneTopic(entry("Group:admins", "Allow", "*", "Read")), "principal type 'Group' is not allowed");
    assertOneProblem(oneTopic(entry("claim:x", "Allow", "*", "Read")), "principal type 'claim' is not allowed");
  }

  @Test
  public void testOperationRules() throws AclFileException {
    assertOneProblem(oneTopic(entry("User:a", "Allow", "*", "ClusterAction")),
                     "operation 'ClusterAction' is not supported for this resource type");
    assertOneProblem(oneTopic(entry("User:a", "Allow", "*", "Reed")), "unknown operation");
    // Kafka's AccessControlEntry itself refuses ANY, so the parser already rejects the file
    AclFileException e = assertThrows(AclFileException.class,
                                      () -> AclTreeParser.parse(bytes(oneTopic(entry("User:a", "Allow", "*", "Any")))));
    assertTrue(e.getMessage(), e.getMessage().contains("operation must not be ANY"));
  }

  @Test
  public void testPermissionAndHost() throws AclFileException {
    assertOneProblem(oneTopic(entry("User:a", "Maybe", "*", "Read")), "permission type must be Allow or Deny");
    assertOneProblem(oneTopic(entry("User:a", "Allow", "localhost", "Read")), "host 'localhost' must be '*' or an IP address");
    assertOneProblem(oneTopic(entry("User:a", "Allow", "300.1.1.1", "Read")), "host '300.1.1.1'");
    assertOneProblem(oneTopic(entry("User:a", "Allow", "fe80::zz", "Read")), "host 'fe80::zz'");
  }

  @Test
  public void testResourceRules() throws AclFileException {
    String describe = entry("User:a", "Allow", "*", "Describe");
    assertOneProblem("{\"kafka-acl\": {\"Cluster\": {\"my-cluster\": {\"version\": 1, \"acls\": [" + describe + "]}}}}",
                     "the Cluster resource must be the literal 'kafka-cluster'");
    assertOneProblem("{\"kafka-acl-extended\": {\"prefixed\": {\"Cluster\": {\"kafka-cluster\": {\"version\": 1, \"acls\": ["
                     + describe + "]}}}}}", "the Cluster resource must be the literal 'kafka-cluster'");
    assertOneProblem("{\"kafka-acl-extended\": {\"prefixed\": {\"Topic\": {\"*\": {\"version\": 1, \"acls\": [" + describe
                     + "]}}}}}", "a prefixed pattern can't be '*'");
    assertOneProblem("{\"kafka-acl\": {\"Topic\": {\"\": {\"version\": 1, \"acls\": [" + describe + "]}}}}",
                     "resource name must not be empty");
    assertOneProblem("{\"kafka-acl\": {\"Group\": {\"team/app\": {\"version\": 1, \"acls\": [" + describe + "]}}}}",
                     "must not contain '/'");
  }

  @Test
  public void testAllProblemsReportedWithLocations() throws AclFileException {
    List<String> problems = problems(oneTopic(entry("Group:x", "Maybe", "host1", "ClusterAction")));
    assertEquals(problems.toString(), 4, problems.size());
    problems.forEach(p -> assertTrue(p, p.startsWith("kafka-acl/Topic/t [")));
  }

  @Test
  public void testIsValidHost() {
    assertTrue(AclValidator.isValidHost("*"));
    assertTrue(AclValidator.isValidHost("192.168.0.1"));
    assertTrue(AclValidator.isValidHost("2001:db8::1"));
    assertFalse(AclValidator.isValidHost("example.com"));
    assertFalse(AclValidator.isValidHost("1.2.3"));
    assertFalse(AclValidator.isValidHost(""));
  }
}
