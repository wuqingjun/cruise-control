/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.acl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import kafka.security.authorizer.AclEntry;
import kafka.security.authorizer.AclEntry$;
import kafka.zk.AclChangeNode;
import kafka.zk.ResourceZNode$;
import kafka.zk.ZkAclChangeStore;
import kafka.zk.ZkAclStore;
import kafka.zk.ZkAclStore$;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import scala.collection.immutable.ListSet$;
import scala.jdk.javaapi.CollectionConverters;

/**
 * The only class of the ACL sync that uses Kafka-version-specific classes (Kafka core 3.5.2:
 * {@code kafka.security.authorizer.AclEntry}, {@code kafka.zk.ZkAclStore}, {@code kafka.zk.ResourceZNode}). Everything it
 * produces is Kafka's own format, so the brokers (Kafka 3.9; same ZooKeeper format) read it as if {@code kafka-acls.sh} had
 * written it. Moving Cruise Control to a newer Kafka only requires changing this class.
 * <p>
 * Paths are relative to the ACL root (the chroot of the authorizer's ZooKeeper connect string).
 */
public final class KafkaAclZkCodec {

  private KafkaAclZkCodec() {
  }

  /** A change notification: create it as a {@code PERSISTENT_SEQUENTIAL} node at {@code pathPrefix} with {@code data}. */
  public static final class ChangeNode {
    private final String _pathPrefix;
    private final byte[] _data;

    ChangeNode(String pathPrefix, byte[] data) {
      _pathPrefix = pathPrefix;
      _data = data;
    }

    /**
     * @return Path prefix of the sequential node, e.g. {@code /kafka-acl-changes/acl_changes_}.
     */
    public String pathPrefix() {
      return _pathPrefix;
    }

    /**
     * @return Node data, e.g. {@code Topic:orders} (literal) or JSON (prefixed).
     */
    public byte[] data() {
      return _data.clone();
    }
  }

  /**
   * @param patternType LITERAL or PREFIXED.
   * @return Root of that ACL subtree, e.g. {@code /kafka-acl} or {@code /kafka-acl-extended/prefixed}.
   */
  public static String rootPath(PatternType patternType) {
    return store(patternType).aclPath();
  }

  /**
   * @param patternType LITERAL or PREFIXED.
   * @param resourceType Resource type.
   * @return Folder of that resource type, e.g. {@code /kafka-acl/Topic}.
   */
  public static String typePath(PatternType patternType, ResourceType resourceType) {
    return store(patternType).path(resourceType);
  }

  /**
   * @param pattern Resource pattern.
   * @return Data node of the resource, e.g. {@code /kafka-acl/Topic/orders}.
   */
  public static String dataPath(ResourcePattern pattern) {
    return store(pattern.patternType()).path(pattern.resourceType(), pattern.name());
  }

  /**
   * @param patternType LITERAL or PREFIXED.
   * @return Folder of that subtree's change notifications, e.g. {@code /kafka-acl-changes}.
   */
  public static String changeRootPath(PatternType patternType) {
    return store(patternType).changeStore().aclChangePath();
  }

  /**
   * @param pattern Resource pattern that changed.
   * @return The change notification to create so that the brokers reload the resource.
   */
  public static ChangeNode changeNode(ResourcePattern pattern) {
    ZkAclChangeStore changeStore = store(pattern.patternType()).changeStore();
    AclChangeNode node = changeStore.createChangeNode(pattern);
    return new ChangeNode(node.path(), node.bytes());
  }

  /**
   * @param patternType LITERAL or PREFIXED.
   * @param data Data of a change notification node.
   * @return The resource the notification refers to (as the brokers decode it).
   */
  public static ResourcePattern decodeChangeNode(PatternType patternType, byte[] data) {
    return store(patternType).changeStore().decode(data);
  }

  /**
   * Encode a resource's entries as its data node content, {@code {"version":1,"acls":[...]}}, in the entries' sorted order.
   *
   * @param entries ACL entries of one resource.
   * @return Data node bytes, exactly as Kafka's authorizer writes them.
   */
  public static byte[] encode(Collection<AclEntryKey> entries) {
    List<AclEntry> aclEntries = new ArrayList<>(entries.size());
    for (AclEntryKey key : new TreeSet<>(entries)) {
      aclEntries.add(new AclEntry(key.toAccessControlEntry()));
    }
    // ListSet keeps insertion (sorted) order, so the JSON array order is stable.
    scala.collection.immutable.Set<AclEntry> scalaSet = ListSet$.MODULE$.from(CollectionConverters.asScala(aclEntries));
    return ResourceZNode$.MODULE$.encode(scalaSet);
  }

  /**
   * Decode a data node's content with Kafka's parser.
   *
   * @param data Data node bytes, {@code {"version":1,"acls":[...]}}.
   * @return The entries, sorted; duplicates collapse (it is a set).
   * @throws IllegalArgumentException If the bytes are not valid ACL JSON (e.g. wrong version, bad principal).
   */
  public static SortedSet<AclEntryKey> decode(byte[] data) {
    Objects.requireNonNull(data, "data");
    SortedSet<AclEntryKey> entries = new TreeSet<>();
    try {
      for (AclEntry aclEntry : CollectionConverters.asJava(AclEntry$.MODULE$.fromBytes(data))) {
        entries.add(AclEntryKey.of(aclEntry));
      }
    } catch (IllegalArgumentException e) {
      throw e;
    } catch (RuntimeException e) {
      throw new IllegalArgumentException("Invalid ACL data: " + e.getMessage(), e);
    }
    return entries;
  }

  /**
   * @param resourceType Resource type.
   * @return Operations Kafka allows for that resource type (without {@link AclOperation#ALL}, which is always allowed).
   */
  public static Set<AclOperation> supportedOperations(ResourceType resourceType) {
    Set<AclOperation> operations = EnumSet.noneOf(AclOperation.class);
    operations.addAll(CollectionConverters.asJava(AclEntry$.MODULE$.supportedOperations(resourceType)));
    return Collections.unmodifiableSet(operations);
  }

  /**
   * @return The concrete resource types Kafka stores ACLs for (one folder each under every subtree).
   */
  public static Set<ResourceType> resourceTypes() {
    Set<ResourceType> types = new LinkedHashSet<>(CollectionConverters.asJava(AclEntry$.MODULE$.ResourceTypes()));
    return Collections.unmodifiableSet(types);
  }

  private static ZkAclStore store(PatternType patternType) {
    if (!AclTree.PATTERN_TYPES.contains(patternType)) {
      throw new IllegalArgumentException("Unsupported pattern type " + patternType);
    }
    return ZkAclStore$.MODULE$.apply(patternType);
  }
}
