/*
 * Copyright 2026 qjwu. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.acl;

import java.nio.charset.StandardCharsets;

/**
 * Sample ACL files for tests. {@link #LOCAL_CLUSTER} is the ACL state of the local test cluster (2026-11-14).
 */
final class AclTestFiles {
  static final String LOCAL_CLUSTER = "{\n"
      + "  \"kafka-acl\": {\n"
      + "    \"Topic\": {\n"
      + "      \"test-authz\": {\"version\": 1, \"acls\": [\n"
      + "        {\"principal\": \"Claim:kafka-writers\", \"permissionType\": \"Allow\", \"host\": \"*\", \"operation\": \"Write\"},\n"
      + "        {\"principal\": \"Claim:kafka-readers\", \"permissionType\": \"Allow\", \"host\": \"*\", \"operation\": \"Read\"},\n"
      + "        {\"principal\": \"Claim:contractors\",   \"permissionType\": \"Deny\",  \"host\": \"*\", \"operation\": \"Read\"}\n"
      + "      ]}\n"
      + "    }\n"
      + "  },\n"
      + "  \"kafka-acl-extended\": {\n"
      + "    \"prefixed\": {\n"
      + "      \"Group\": {\n"
      + "        \"saml-test-\": {\"version\": 1, \"acls\": [\n"
      + "          {\"principal\": \"Claim:kafka-readers\", \"permissionType\": \"Allow\", \"host\": \"*\", \"operation\": \"Read\"}\n"
      + "        ]}\n"
      + "      }\n"
      + "    }\n"
      + "  }\n"
      + "}\n";

  private AclTestFiles() {
  }

  static byte[] bytes(String json) {
    return json.getBytes(StandardCharsets.UTF_8);
  }

  /**
   * @param leafAcls The content of the "acls" array.
   * @return A file with one literal topic "t" whose leaf has the given ACL entries.
   */
  static String oneTopic(String leafAcls) {
    return "{\"kafka-acl\": {\"Topic\": {\"t\": {\"version\": 1, \"acls\": [" + leafAcls + "]}}}}";
  }

  /**
   * @param principal Principal.
   * @param permission Permission type name.
   * @param host Host.
   * @param operation Operation name.
   * @return One ACL entry as JSON.
   */
  static String entry(String principal, String permission, String host, String operation) {
    return String.format("{\"principal\": \"%s\", \"permissionType\": \"%s\", \"host\": \"%s\", \"operation\": \"%s\"}",
                         principal, permission, host, operation);
  }
}
