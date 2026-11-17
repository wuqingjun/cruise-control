/*
 * Copyright 2026 qjwu. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.acl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;

/**
 * ZooKeeper ACLs (znode permissions) for the brokers' ACL storage, so that only Cruise Control can change Kafka ACLs in
 * ZooKeeper (design {@code zk-tls-acl-design.md}):
 * <ul>
 *   <li>{@link #topFolderAcls()} for {@code /kafka-acl}, {@code /kafka-acl-extended}, {@code /kafka-acl-extended/prefixed}:
 *       Cruise Control all, brokers read + create (broker startup re-creates the resource-type folders, and ZooKeeper checks
 *       CREATE on the parent before "exists").</li>
 *   <li>{@link #brokerReadAcls()} for resource-type folders, resource data nodes, change folders and notifications:
 *       Cruise Control all, brokers read.</li>
 *   <li>{@link #ownerOnlyAcls()} for Cruise Control's own state folder, state node and lock: Cruise Control only.</li>
 * </ul>
 * Nobody else is listed. Brokers get no DELETE: Cruise Control purges the notifications itself. A ZooKeeper super user (e.g.
 * an operator certificate) bypasses all of this.
 * <p>
 * {@link #open()} keeps the previous behavior: every node is created with {@code world:anyone} all permissions and nothing
 * is changed on existing nodes.
 */
public final class AclZnodePolicy {
  private static final int BROKER_TOP_FOLDER_PERMS = ZooDefs.Perms.READ | ZooDefs.Perms.CREATE;

  private final Id _owner;
  private final List<Id> _brokers;

  private AclZnodePolicy(Id owner, List<Id> brokers) {
    _owner = owner;
    _brokers = brokers;
  }

  /**
   * @return The policy without protection: everything open, existing nodes untouched.
   */
  public static AclZnodePolicy open() {
    return new AclZnodePolicy(null, List.of());
  }

  /**
   * @param owner Cruise Control's own ZooKeeper identity, e.g. {@code x509:O=LocalDev,CN=cruise-control}.
   * @param brokers The brokers' ZooKeeper identities.
   * @return The protecting policy.
   */
  public static AclZnodePolicy protect(Id owner, Collection<Id> brokers) {
    Objects.requireNonNull(owner, "owner");
    if (brokers.isEmpty()) {
      throw new IllegalArgumentException("At least one broker identity is required: brokers must be able to read the ACLs.");
    }
    return new AclZnodePolicy(owner, List.copyOf(brokers));
  }

  /**
   * @return Whether znode ACLs are set and enforced on existing nodes.
   */
  public boolean isProtecting() {
    return _owner != null;
  }

  /**
   * @return Cruise Control's identity, or {@code null} for the open policy.
   */
  public Id owner() {
    return _owner;
  }

  /**
   * @return ACLs for the three top-level ACL folders.
   */
  public List<ACL> topFolderAcls() {
    return acls(BROKER_TOP_FOLDER_PERMS);
  }

  /**
   * @return ACLs for resource-type folders, data nodes, change folders and notification nodes.
   */
  public List<ACL> brokerReadAcls() {
    return acls(ZooDefs.Perms.READ);
  }

  /**
   * @return ACLs for Cruise Control's state folder, state node and lock.
   */
  public List<ACL> ownerOnlyAcls() {
    return acls(0);
  }

  private List<ACL> acls(int brokerPerms) {
    if (!isProtecting()) {
      return mutable(ZooDefs.Ids.OPEN_ACL_UNSAFE);
    }
    List<ACL> acls = new ArrayList<>();
    acls.add(new ACL(ZooDefs.Perms.ALL, _owner));
    if (brokerPerms != 0) {
      for (Id broker : _brokers) {
        if (!broker.equals(_owner)) {
          acls.add(new ACL(brokerPerms, broker));
        }
      }
    }
    return Collections.unmodifiableList(acls);
  }

  /**
   * ZooKeeper's validateACL calls contains(null), which immutable List.of / List.copyOf lists reject with an NPE.
   *
   * @param acls ACLs.
   * @return An unmodifiable copy that accepts contains(null).
   */
  private static List<ACL> mutable(List<ACL> acls) {
    return Collections.unmodifiableList(new ArrayList<>(acls));
  }

  /**
   * Parse broker identities: {@code ;}-separated (certificate DNs contain commas), all in the given scheme.
   *
   * @param identities e.g. {@code O=LocalDev,CN=broker1;O=LocalDev,CN=broker2}.
   * @param defaultScheme ZooKeeper auth scheme, e.g. {@code x509}.
   * @return The identities.
   */
  public static List<Id> parseIdentities(String identities, String defaultScheme) {
    List<Id> ids = new ArrayList<>();
    if (identities == null) {
      return ids;
    }
    for (String entry : identities.split(";")) {
      String trimmed = entry.trim();
      if (trimmed.isEmpty()) {
        continue;
      }
      ids.add(new Id(defaultScheme, trimmed));
    }
    return ids;
  }

  @Override
  public String toString() {
    if (!isProtecting()) {
      return "open (world:anyone)";
    }
    List<String> brokers = new ArrayList<>();
    _brokers.forEach(b -> brokers.add(b.getScheme() + ":" + b.getId()));
    return String.format("owner %s:%s (all), brokers %s (read; create on top folders)", _owner.getScheme(), _owner.getId(), brokers);
  }
}
