/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.acl;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.Resource;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.SecurityUtils;

/**
 * Content rules for the ACL file, checked on the whole tree before anything is written. All problems are reported at
 * once; if there is any, the file is rejected and nothing is applied.
 * <ul>
 *   <li>Resource names: not empty; a {@code Cluster} resource must be the literal {@code kafka-cluster}; a prefixed pattern
 *       can't be {@code *}.</li>
 *   <li>Principal: {@code type:name}, with a type from the configured list (case-sensitive).</li>
 *   <li>Operation: a concrete operation Kafka supports for the resource type, or {@code All}.</li>
 *   <li>Permission: {@code Allow} or {@code Deny}.</li>
 *   <li>Host: {@code *} or an IP address (no host names).</li>
 * </ul>
 * Names and principals are never normalized: Kafka compares them exactly (case-sensitive).
 */
public final class AclValidator {
  private static final Pattern IPV4 = Pattern.compile("^(25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)(\\.(25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)){3}$");
  private static final Pattern IPV6_CHARS = Pattern.compile("^[0-9a-fA-F:.]+$");
  private static final String WILDCARD = ResourcePattern.WILDCARD_RESOURCE;

  private AclValidator() {
  }

  /**
   * Validate a tree.
   *
   * @param tree ACLs parsed from the file.
   * @param principalTypes Allowed principal types, e.g. {@code User}, {@code Claim}, {@code Certificate}.
   * @return All problems found, each prefixed with its location (e.g. {@code kafka-acl/Topic/orders}); empty if valid.
   */
  public static List<String> validate(AclTree tree, Collection<String> principalTypes) {
    Set<String> types = new HashSet<>(principalTypes);
    List<String> problems = new ArrayList<>();
    for (ResourcePattern pattern : tree.resourcePatterns()) {
      String location = AclTreeParser.location(pattern);
      validateResource(pattern, location, problems);
      Set<AclOperation> supported = KafkaAclZkCodec.supportedOperations(pattern.resourceType());
      for (AclEntryKey entry : tree.entries(pattern)) {
        validateEntry(entry, supported, types, String.format("%s [%s]", location, entry), problems);
      }
    }
    return problems;
  }

  private static void validateResource(ResourcePattern pattern, String location, List<String> problems) {
    if (pattern.name().isEmpty()) {
      problems.add(location + ": resource name must not be empty.");
    }
    if (pattern.name().indexOf('/') >= 0) {
      problems.add(location + ": resource name must not contain '/' (it is a ZooKeeper node name).");
    }
    if (pattern.resourceType() == ResourceType.CLUSTER
        && (pattern.patternType() != PatternType.LITERAL || !Resource.CLUSTER_NAME.equals(pattern.name()))) {
      problems.add(String.format("%s: the Cluster resource must be the literal '%s'.", location, Resource.CLUSTER_NAME));
    }
    if (pattern.patternType() == PatternType.PREFIXED && WILDCARD.equals(pattern.name())) {
      problems.add(location + ": a prefixed pattern can't be '*'; use the literal '*' for all resources.");
    }
  }

  private static void validateEntry(AclEntryKey entry, Set<AclOperation> supported, Set<String> principalTypes, String location,
                                    List<String> problems) {
    try {
      KafkaPrincipal principal = SecurityUtils.parseKafkaPrincipal(entry.principal());
      if (!principalTypes.contains(principal.getPrincipalType())) {
        problems.add(String.format("%s: principal type '%s' is not allowed (allowed: %s).", location, principal.getPrincipalType(),
                                   principalTypes));
      }
      if (principal.getName().isEmpty()) {
        problems.add(location + ": principal name must not be empty.");
      }
    } catch (IllegalArgumentException e) {
      problems.add(String.format("%s: principal '%s' must be 'type:name'.", location, entry.principal()));
    }
    AclOperation operation = entry.operation();
    if (operation == AclOperation.UNKNOWN || operation == AclOperation.ANY) {
      problems.add(location + ": unknown operation.");
    } else if (operation != AclOperation.ALL && !supported.contains(operation)) {
      problems.add(String.format("%s: operation '%s' is not supported for this resource type (supported: All, %s).", location,
                                 SecurityUtils.operationName(operation), operationNames(supported)));
    }
    AclPermissionType permission = entry.permissionType();
    if (permission != AclPermissionType.ALLOW && permission != AclPermissionType.DENY) {
      problems.add(location + ": permission type must be Allow or Deny.");
    }
    if (!isValidHost(entry.host())) {
      problems.add(String.format("%s: host '%s' must be '*' or an IP address.", location, entry.host()));
    }
  }

  static boolean isValidHost(String host) {
    if (WILDCARD.equals(host) || IPV4.matcher(host).matches()) {
      return true;
    }
    if (host.indexOf(':') < 0 || !IPV6_CHARS.matcher(host).matches()) {
      return false;
    }
    try {
      // Only hex digits, ':' and '.': parsed as an IPv6 literal, never looked up by name.
      InetAddress.getByName(host);
      return true;
    } catch (UnknownHostException | SecurityException e) {
      return false;
    }
  }

  private static String operationNames(Set<AclOperation> operations) {
    List<String> names = new ArrayList<>();
    operations.forEach(op -> names.add(SecurityUtils.operationName(op)));
    names.sort(String::compareTo);
    return String.join(", ", names);
  }
}
