/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.acl;

import java.util.Comparator;
import java.util.Objects;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.utils.SecurityUtils;

/**
 * One ACL entry of a resource: principal, operation, permission type and host. Comparable, so entries are kept sorted by
 * (principal, operation, permissionType, host), using the names Kafka writes to ZooKeeper (e.g. {@code Read}, {@code Allow}).
 * The order only serves comparison and a stable output; Kafka itself treats a resource's entries as a set.
 */
public final class AclEntryKey implements Comparable<AclEntryKey> {
  private static final Comparator<AclEntryKey> ORDER = Comparator.comparing(AclEntryKey::principal)
      .thenComparing(e -> SecurityUtils.operationName(e.operation()))
      .thenComparing(e -> SecurityUtils.permissionTypeName(e.permissionType()))
      .thenComparing(AclEntryKey::host);

  private final String _principal;
  private final AclOperation _operation;
  private final AclPermissionType _permissionType;
  private final String _host;

  /**
   * @param principal Principal as {@code type:name}, e.g. {@code Claim:kafka-readers}.
   * @param operation Operation.
   * @param permissionType Permission type.
   * @param host Host, {@code *} for any.
   */
  public AclEntryKey(String principal, AclOperation operation, AclPermissionType permissionType, String host) {
    _principal = Objects.requireNonNull(principal, "principal");
    _operation = Objects.requireNonNull(operation, "operation");
    _permissionType = Objects.requireNonNull(permissionType, "permissionType");
    _host = Objects.requireNonNull(host, "host");
  }

  /**
   * @param entry Kafka access control entry.
   * @return The key for the given entry.
   */
  public static AclEntryKey of(AccessControlEntry entry) {
    return new AclEntryKey(entry.principal(), entry.operation(), entry.permissionType(), entry.host());
  }

  /**
   * @return This entry as a Kafka {@link AccessControlEntry}.
   */
  public AccessControlEntry toAccessControlEntry() {
    return new AccessControlEntry(_principal, _host, _operation, _permissionType);
  }

  public String principal() {
    return _principal;
  }

  public AclOperation operation() {
    return _operation;
  }

  public AclPermissionType permissionType() {
    return _permissionType;
  }

  public String host() {
    return _host;
  }

  @Override
  public int compareTo(AclEntryKey other) {
    return ORDER.compare(this, other);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof AclEntryKey)) {
      return false;
    }
    AclEntryKey that = (AclEntryKey) o;
    return _principal.equals(that._principal) && _operation == that._operation
           && _permissionType == that._permissionType && _host.equals(that._host);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_principal, _operation, _permissionType, _host);
  }

  @Override
  public String toString() {
    return String.format("%s %s %s from %s", _principal, SecurityUtils.permissionTypeName(_permissionType),
                         SecurityUtils.operationName(_operation), _host);
  }
}
