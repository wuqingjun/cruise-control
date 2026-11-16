/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.acl;

import com.linkedin.kafka.cruisecontrol.acl.AclTreeDiff.ChangeType;
import com.linkedin.kafka.cruisecontrol.acl.AclTreeDiff.Result;
import com.linkedin.kafka.cruisecontrol.metricsreporter.utils.CCEmbeddedZookeeper;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * {@link AclZkStore} against an embedded ZooKeeper, with the ACL root at the chroot {@code /kafka} like the local cluster.
 */
public class AclZkStoreTest {
  private static final String CHROOT = "/kafka";
  private static final int TIMEOUT_MS = 30_000;
  private static final AclEntryKey READ_A = new AclEntryKey("Claim:a", AclOperation.READ, AclPermissionType.ALLOW, "*");
  private static final AclEntryKey WRITE_B = new AclEntryKey("Claim:b", AclOperation.WRITE, AclPermissionType.ALLOW, "*");
  private static final AclEntryKey DENY_C = new AclEntryKey("Claim:c", AclOperation.READ, AclPermissionType.DENY, "*");
  private static final ResourcePattern T1 = new ResourcePattern(ResourceType.TOPIC, "t1", PatternType.LITERAL);
  private static final ResourcePattern T2 = new ResourcePattern(ResourceType.TOPIC, "t2", PatternType.LITERAL);
  private static final ResourcePattern PG = new ResourcePattern(ResourceType.GROUP, "g-", PatternType.PREFIXED);

  private CCEmbeddedZookeeper _zkServer;
  private ZooKeeper _zk;
  private AclZkStore _store;

  /**
   * Start an embedded ZooKeeper with the chroot and connect a store to it.
   *
   * @throws Exception On setup errors.
   */
  @Before
  public void setUp() throws Exception {
    _zkServer = new CCEmbeddedZookeeper();
    ZooKeeper rootZk = AclZkStore.connect(_zkServer.connectionString(), null, TIMEOUT_MS, TIMEOUT_MS);
    rootZk.create(CHROOT, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    rootZk.close();
    _zk = connect();
    _store = new AclZkStore(_zk, 900_000);
  }

  /**
   * Close the store and stop ZooKeeper.
   */
  @After
  public void tearDown() {
    if (_store != null) {
      _store.close();
    }
    if (_zkServer != null) {
      _zkServer.close();
    }
  }

  private ZooKeeper connect() throws Exception {
    return AclZkStore.connect(_zkServer.connectionString() + CHROOT, null, TIMEOUT_MS, TIMEOUT_MS);
  }

  /** Creates the folders a broker creates at startup (AclAuthorizer -> createAclPaths). */
  private void createBrokerStructure() throws Exception {
    for (PatternType pt : AclTree.PATTERN_TYPES) {
      createRecursive(KafkaAclZkCodec.rootPath(pt));
      for (ResourceType type : KafkaAclZkCodec.resourceTypes()) {
        createRecursive(KafkaAclZkCodec.typePath(pt, type));
      }
      createRecursive(KafkaAclZkCodec.changeRootPath(pt));
    }
  }

  private void createRecursive(String path) throws Exception {
    StringBuilder current = new StringBuilder();
    for (String part : path.substring(1).split("/")) {
      current.append('/').append(part);
      if (_zk.exists(current.toString(), false) == null) {
        _zk.create(current.toString(), new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      }
    }
  }

  /**
   * Writes a data node directly, the way a broker would.
   *
   * @param pattern Resource.
   * @param entries Entries.
   * @throws Exception On ZooKeeper errors.
   */
  private void writeLikeABroker(ResourcePattern pattern, List<AclEntryKey> entries) throws Exception {
    _zk.create(KafkaAclZkCodec.dataPath(pattern), KafkaAclZkCodec.encode(entries), ZooDefs.Ids.OPEN_ACL_UNSAFE,
               CreateMode.PERSISTENT);
  }

  private List<ResourcePattern> notifications() throws Exception {
    List<ResourcePattern> patterns = new ArrayList<>();
    for (PatternType pt : AclTree.PATTERN_TYPES) {
      List<String> children = new ArrayList<>(_zk.getChildren(KafkaAclZkCodec.changeRootPath(pt), false));
      children.sort(String::compareTo);
      for (String child : children) {
        byte[] data = _zk.getData(KafkaAclZkCodec.changeRootPath(pt) + "/" + child, false, null);
        patterns.add(KafkaAclZkCodec.decodeChangeNode(pt, data));
      }
    }
    return patterns;
  }

  private static AclTree tree(Object... patternsAndEntries) {
    AclTree tree = new AclTree();
    for (int i = 0; i < patternsAndEntries.length; i += 2) {
      @SuppressWarnings("unchecked")
      List<AclEntryKey> entries = (List<AclEntryKey>) patternsAndEntries[i + 1];
      tree.put((ResourcePattern) patternsAndEntries[i], entries);
    }
    return tree;
  }

  private AclZkStore.ApplyResult sync(AclTree desired, String sha) throws Exception {
    AclZkStore.Snapshot snapshot = _store.readCurrent();
    Result diff = AclTreeDiff.diff(snapshot.tree(), desired, Set.of());
    Optional<Integer> stateVersion = _store.readState().map(AclZkStore.VersionedState::version);
    return _store.apply(diff.changes(), snapshot, new AclStateNode(sha, 1L, "acls.json", List.of()), stateVersion);
  }

  @Test
  public void testMissingStructure() throws Exception {
    assertEquals(2 * (2 + KafkaAclZkCodec.resourceTypes().size()), _store.missingStructure().size());
    createBrokerStructure();
    assertEquals(List.of(), _store.missingStructure());
  }

  @Test
  public void testReadCurrentReadsWhatBrokersWrote() throws Exception {
    createBrokerStructure();
    assertEquals(0, _store.readCurrent().tree().numResources());
    writeLikeABroker(T1, List.of(READ_A, WRITE_B));
    writeLikeABroker(PG, List.of(DENY_C));

    AclZkStore.Snapshot snapshot = _store.readCurrent();

    assertEquals(tree(T1, List.of(READ_A, WRITE_B), PG, List.of(DENY_C)), snapshot.tree());
    assertEquals(Optional.of(0), snapshot.version(T1));
    assertEquals(Optional.empty(), snapshot.version(T2));
  }

  @Test
  public void testFirstApplyCreatesNodesNotificationsAndState() throws Exception {
    createBrokerStructure();
    AclTree desired = tree(T1, List.of(READ_A), T2, List.of(WRITE_B), PG, List.of(DENY_C));

    AclZkStore.ApplyResult result = sync(desired, "sha-1");

    assertEquals(1, result.batches());
    assertEquals(3, result.notifications());
    assertEquals(desired, _store.readCurrent().tree());
    assertEquals(new HashSet<>(List.of(T1, T2, PG)), new HashSet<>(notifications()));
    assertEquals("sha-1", _store.readState().get().state().sha256());
    // Kafka 3.9's format/decoder reads the node (checked via the codec, which uses Kafka's parser)
    assertEquals(Set.of(READ_A), KafkaAclZkCodec.decode(_zk.getData(KafkaAclZkCodec.dataPath(T1), false, null)));
  }

  @Test
  public void testUpdateDeleteAndNoOpReapply() throws Exception {
    createBrokerStructure();
    sync(tree(T1, List.of(READ_A), T2, List.of(WRITE_B)), "sha-1");
    int before = notifications().size();

    AclZkStore.ApplyResult result = sync(tree(T1, List.of(READ_A, DENY_C)), "sha-2");

    assertEquals(2, result.notifications());
    assertEquals(tree(T1, List.of(READ_A, DENY_C)), _store.readCurrent().tree());
    assertNull(_zk.exists(KafkaAclZkCodec.dataPath(T2), false));
    assertEquals(before + 2, notifications().size());
    assertEquals("sha-2", _store.readState().get().state().sha256());

    // Same desired tree again: no ACL operations, no notifications, only the state node
    AclZkStore.ApplyResult again = sync(tree(T1, List.of(READ_A, DENY_C)), "sha-3");
    assertEquals(0, again.notifications());
    assertEquals(1, again.operations());
    assertEquals(before + 2, notifications().size());
  }

  @Test
  public void testConflictLeavesBatchUnapplied() throws Exception {
    createBrokerStructure();
    sync(tree(T1, List.of(READ_A)), "sha-1");
    AclZkStore.Snapshot snapshot = _store.readCurrent();
    Result diff = AclTreeDiff.diff(snapshot.tree(), tree(T1, List.of(WRITE_B), T2, List.of(READ_A)), Set.of());
    int notificationsBefore = notifications().size();
    int stateVersion = _store.readState().get().version();

    // Someone (e.g. kafka-acls.sh through a broker) rewrites T1 after our read
    _zk.setData(KafkaAclZkCodec.dataPath(T1), KafkaAclZkCodec.encode(List.of(DENY_C)), -1);

    AclZkConflictException e = assertThrows(AclZkConflictException.class, () -> _store.apply(
        diff.changes(), snapshot, new AclStateNode("sha-2", 2L, "acls.json", List.of()), Optional.of(stateVersion)));

    assertEquals(0, e.appliedBatches());
    assertTrue(e.getMessage(), e.getMessage().contains("/kafka-acl/Topic/t1"));
    // Nothing of the batch happened: T2 not created, no notification, state unchanged
    assertNull(_zk.exists(KafkaAclZkCodec.dataPath(T2), false));
    assertEquals(notificationsBefore, notifications().size());
    assertEquals("sha-1", _store.readState().get().state().sha256());

    // Starting over (read again, compare, apply) succeeds
    sync(tree(T1, List.of(WRITE_B), T2, List.of(READ_A)), "sha-2");
    assertEquals(tree(T1, List.of(WRITE_B), T2, List.of(READ_A)), _store.readCurrent().tree());
  }

  @Test
  public void testSplitBatchesAndCrashBetweenBatches() throws Exception {
    createBrokerStructure();
    AclZkStore small = new AclZkStore(_zk, 1200);
    AclTree desired = new AclTree();
    for (int i = 0; i < 20; i++) {
      desired.put(new ResourcePattern(ResourceType.TOPIC, String.format("topic-%02d", i), PatternType.LITERAL),
                  List.of(READ_A, WRITE_B, DENY_C));
    }
    AclZkStore.Snapshot snapshot = small.readCurrent();
    Result diff = AclTreeDiff.diff(snapshot.tree(), desired, Set.of());
    List<AclZkStore.Batch> batches = small.buildBatches(diff.changes(), snapshot,
                                                        new AclStateNode("sha-1", 1L, "acls.json", List.of()), Optional.empty());
    assertTrue("expected several batches, got " + batches.size(), batches.size() >= 3);
    batches.forEach(b -> assertTrue(b.bytes() <= 1200));
    // State node only in the last batch
    assertTrue(batches.get(batches.size() - 1).ops().stream().anyMatch(op -> op.getPath().equals(AclZkStore.STATE_NODE)));

    // "Crash" after the first batch
    AclZkStore.ApplyResult partial = small.apply(diff.changes(), snapshot, new AclStateNode("sha-1", 1L, "acls.json", List.of()),
                                                 Optional.empty(), 1);
    assertEquals(1, partial.batches());
    int appliedResources = small.readCurrent().tree().numResources();
    assertTrue(appliedResources > 0 && appliedResources < 20);
    assertFalse("state must not move before the last batch", small.readState().isPresent());

    // Restart the whole process: read, compare, apply what is left
    AclZkStore.Snapshot again = small.readCurrent();
    Result rest = AclTreeDiff.diff(again.tree(), desired, Set.of());
    assertEquals(20 - appliedResources, rest.count(ChangeType.ADDED));
    small.apply(rest.changes(), again, new AclStateNode("sha-1", 2L, "acls.json", List.of()), Optional.empty());
    assertEquals(desired, small.readCurrent().tree());
    assertEquals("sha-1", small.readState().get().state().sha256());
    assertEquals(20, notifications().size());
  }

  @Test
  public void testExistingNodeWithoutEntriesIsRewritten() throws Exception {
    createBrokerStructure();
    _zk.create(KafkaAclZkCodec.dataPath(T1), new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    assertEquals(0, _store.readCurrent().tree().numResources());

    sync(tree(T1, List.of(READ_A)), "sha-1");

    assertEquals(tree(T1, List.of(READ_A)), _store.readCurrent().tree());
  }

  @Test
  public void testResourceTooLargeForOneBatch() throws Exception {
    createBrokerStructure();
    AclZkStore tiny = new AclZkStore(_zk, 100);
    Result diff = AclTreeDiff.diff(new AclTree(), tree(T1, List.of(READ_A, WRITE_B, DENY_C)), Set.of());
    assertThrows(IllegalArgumentException.class, () -> tiny.buildBatches(
        diff.changes(), tiny.readCurrent(), new AclStateNode("s", 1L, "f", List.of()), Optional.empty()));
  }

  @Test
  public void testInvalidDataNodeReported() throws Exception {
    createBrokerStructure();
    _zk.create(KafkaAclZkCodec.dataPath(T1), "{\"version\":7,\"acls\":[]}".getBytes(StandardCharsets.UTF_8), ZooDefs.Ids.OPEN_ACL_UNSAFE,
               CreateMode.PERSISTENT);
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> _store.readCurrent());
    assertTrue(e.getMessage(), e.getMessage().startsWith("/kafka-acl/Topic/t1"));
  }

  @Test
  public void testLock() throws Exception {
    assertTrue(_store.tryLock("cc-1"));
    try (AclZkStore other = new AclZkStore(connect(), 900_000)) {
      assertFalse(other.tryLock("cc-2"));
      other.unlock();
      assertFalse("unlock by a non-owner must not remove the lock", other.tryLock("cc-2"));
      _store.unlock();
      assertTrue(other.tryLock("cc-2"));
    }
    // other's session closed: its ephemeral lock is gone
    long deadline = System.currentTimeMillis() + TIMEOUT_MS;
    while (_zk.exists(AclZkStore.LOCK_NODE, false) != null && System.currentTimeMillis() < deadline) {
      Thread.sleep(50);
    }
    assertTrue(_store.tryLock("cc-1"));
  }

  @Test
  public void testStateVersionConflict() throws Exception {
    createBrokerStructure();
    sync(tree(T1, List.of(READ_A)), "sha-1");
    AclZkStore.Snapshot snapshot = _store.readCurrent();
    int version = _store.readState().get().version();
    _zk.setData(AclZkStore.STATE_NODE, new AclStateNode("other", 9L, "x", List.of()).toBytes(), -1);
    Result diff = AclTreeDiff.diff(snapshot.tree(), tree(T1, List.of(WRITE_B)), Set.of());
    assertThrows(AclZkConflictException.class, () -> _store.apply(
        diff.changes(), snapshot, new AclStateNode("sha-2", 2L, "acls.json", List.of()), Optional.of(version)));
    assertEquals(Set.of(READ_A), KafkaAclZkCodec.decode(_zk.getData(KafkaAclZkCodec.dataPath(T1), false, null)));
  }

  @Test
  public void testConnectTimeout() {
    assertThrows(java.io.IOException.class, () -> AclZkStore.connect("127.0.0.1:1", null, 2000, 500));
  }

  @Test
  public void testNoNodeTypeFolderIsSkipped() throws KeeperException, InterruptedException {
    assertEquals(0, _store.readCurrent().tree().numResources());
  }
}
