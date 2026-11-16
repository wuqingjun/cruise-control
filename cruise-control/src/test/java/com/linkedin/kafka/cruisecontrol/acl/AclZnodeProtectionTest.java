/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.acl;

import com.linkedin.kafka.cruisecontrol.metricsreporter.utils.CCEmbeddedZookeeper;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
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
import org.apache.kafka.common.utils.Time;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Znode ACLs on the ACL subtree ({@link AclZnodePolicy}): only Cruise Control can change ACLs in ZooKeeper; brokers can
 * read and re-create the folders at startup; anybody else can do nothing. The embedded ZooKeeper has no TLS, so the
 * identities use the {@code digest} scheme; the policy is the same for {@code x509}.
 */
public class AclZnodeProtectionTest {
  private static final String CHROOT = "/kafka";
  private static final int TIMEOUT_MS = 30_000;
  private static final String SCHEME = "digest";
  private static final AclEntryKey READ_A = new AclEntryKey("Claim:a", AclOperation.READ, AclPermissionType.ALLOW, "*");
  private static final AclEntryKey WRITE_B = new AclEntryKey("Claim:b", AclOperation.WRITE, AclPermissionType.ALLOW, "*");
  private static final ResourcePattern T1 = new ResourcePattern(ResourceType.TOPIC, "t1", PatternType.LITERAL);
  private static final ResourcePattern LEGACY = new ResourcePattern(ResourceType.TOPIC, "legacy", PatternType.LITERAL);
  private static final ResourcePattern PG = new ResourcePattern(ResourceType.GROUP, "g-", PatternType.PREFIXED);

  private CCEmbeddedZookeeper _zkServer;
  private final List<ZooKeeper> _sessions = new ArrayList<>();
  private ZooKeeper _cc;
  private ZooKeeper _broker;
  private ZooKeeper _mallory;
  private ZooKeeper _anonymous;
  private AclZkStore _store;

  /**
   * Embedded ZooKeeper with the broker folder structure (open, as brokers create it); sessions for Cruise Control,
   * a broker, another authenticated client and an unauthenticated client.
   *
   * @throws Exception On setup errors.
   */
  @Before
  public void setUp() throws Exception {
    _zkServer = new CCEmbeddedZookeeper();
    ZooKeeper rootZk = AclZkStore.connect(_zkServer.connectionString(), null, TIMEOUT_MS, TIMEOUT_MS);
    rootZk.create(CHROOT, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    rootZk.close();
    _cc = session("cruise-control");
    _broker = session("broker1");
    _mallory = session("mallory");
    _anonymous = session(null);
    for (PatternType pt : AclTree.PATTERN_TYPES) {
      createRecursive(_broker, KafkaAclZkCodec.rootPath(pt));
      for (ResourceType type : KafkaAclZkCodec.resourceTypes()) {
        createRecursive(_broker, KafkaAclZkCodec.typePath(pt, type));
      }
      createRecursive(_broker, KafkaAclZkCodec.changeRootPath(pt));
    }
    // a resource written before protection, as the brokers / kafka-acls.sh did
    _broker.create(KafkaAclZkCodec.dataPath(LEGACY), KafkaAclZkCodec.encode(List.of(READ_A)), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                   CreateMode.PERSISTENT);
    // digest: whoAmI returns only the user name, the ACL id is user:hash (for x509 both are the certificate DN)
    _store = new AclZkStore(_cc, 900_000, AclZnodePolicy.protect(digest("cruise-control"), List.of(digest("broker1"), digest("broker2"))));
  }

  /**
   * Close the sessions and stop ZooKeeper.
   *
   * @throws Exception On errors.
   */
  @After
  public void tearDown() throws Exception {
    for (ZooKeeper zk : _sessions) {
      zk.close();
    }
    if (_zkServer != null) {
      _zkServer.close();
    }
  }

  private static String password(String user) {
    return user + "-secret";
  }

  private static Id digest(String user) throws Exception {
    return new Id(SCHEME, DigestAuthenticationProvider.generateDigest(user + ":" + password(user)));
  }

  private ZooKeeper session(String user) throws IOException {
    ZooKeeper zk = AclZkStore.connect(_zkServer.connectionString() + CHROOT, null, TIMEOUT_MS, TIMEOUT_MS);
    if (user != null) {
      zk.addAuthInfo(SCHEME, (user + ":" + password(user)).getBytes(StandardCharsets.UTF_8));
    }
    _sessions.add(zk);
    return zk;
  }

  private static void createRecursive(ZooKeeper zk, String path) throws Exception {
    StringBuilder current = new StringBuilder();
    for (String part : path.substring(1).split("/")) {
      current.append('/').append(part);
      if (zk.exists(current.toString(), false) == null) {
        zk.create(current.toString(), new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      }
    }
  }

  /**
   * Like the brokers' createRecursive(throwIfPathExists = false): only NODEEXISTS counts as success.
   *
   * @param zk Session.
   * @param path Folder.
   * @throws Exception Anything else than NODEEXISTS, e.g. NOAUTH.
   */
  private static void createLikeBrokerStartup(ZooKeeper zk, String path) throws Exception {
    try {
      zk.create(path, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    } catch (KeeperException.NodeExistsException e) {
      // expected: the folder exists
    }
  }

  private void apply(AclTree desired, String sha) throws Exception {
    AclZkStore.Snapshot snapshot = _store.readCurrent();
    AclTreeDiff.Result diff = AclTreeDiff.diff(snapshot.tree(), desired, Set.of());
    Optional<Integer> stateVersion = _store.readState().map(AclZkStore.VersionedState::version);
    _store.apply(diff.changes(), snapshot, new AclStateNode(sha, 1L, "acls.json", List.of()), stateVersion);
  }

  private static AclTree tree(ResourcePattern pattern, List<AclEntryKey> entries) {
    AclTree tree = new AclTree();
    tree.put(pattern, entries);
    return tree;
  }

  private static Set<ACL> acls(ZooKeeper zk, String path) throws Exception {
    return new HashSet<>(zk.getACL(path, null));
  }

  private static void assertNoAuth(String what, ThrowingRunnable action) {
    assertThrows(what, KeeperException.NoAuthException.class, action::run);
  }

  @FunctionalInterface
  private interface ThrowingRunnable {
    void run() throws Exception;
  }

  @Test
  public void testAuthenticatedId() throws Exception {
    assertEquals(Optional.of(new Id(SCHEME, "cruise-control")), _store.authenticatedId(SCHEME));
    assertEquals(Optional.empty(), AclZkStore.authenticatedId(_anonymous, SCHEME));
    assertEquals(Optional.empty(), AclZkStore.authenticatedId(_cc, "x509"));
  }

  @Test
  public void testEnsureZnodeAclsIsIdempotent() throws Exception {
    // 3 top folders + 2 x types + 2 change folders + legacy data node (state folder / node don't exist yet)
    int expected = 3 + 2 * KafkaAclZkCodec.resourceTypes().size() + 2 + 1;
    assertEquals(expected, _store.ensureZnodeAcls());
    assertEquals(0, _store.ensureZnodeAcls());

    Id cc = digest("cruise-control");
    Set<ACL> top = Set.of(new ACL(ZooDefs.Perms.ALL, cc), new ACL(ZooDefs.Perms.READ | ZooDefs.Perms.CREATE, digest("broker1")),
                          new ACL(ZooDefs.Perms.READ | ZooDefs.Perms.CREATE, digest("broker2")));
    Set<ACL> read = Set.of(new ACL(ZooDefs.Perms.ALL, cc), new ACL(ZooDefs.Perms.READ, digest("broker1")),
                           new ACL(ZooDefs.Perms.READ, digest("broker2")));
    assertEquals(top, acls(_cc, "/kafka-acl"));
    assertEquals(top, acls(_cc, "/kafka-acl-extended"));
    assertEquals(top, acls(_cc, "/kafka-acl-extended/prefixed"));
    assertEquals(read, acls(_cc, "/kafka-acl/Topic"));
    assertEquals(read, acls(_cc, KafkaAclZkCodec.dataPath(LEGACY)));
    assertEquals(read, acls(_cc, KafkaAclZkCodec.changeRootPath(PatternType.PREFIXED)));
  }

  @Test
  public void testOnlyCruiseControlCanWriteAcls() throws Exception {
    _store.ensureZnodeAcls();
    apply(tree(T1, List.of(READ_A)), "sha-1");
    String t1 = KafkaAclZkCodec.dataPath(T1);
    String changes = KafkaAclZkCodec.changeRootPath(PatternType.LITERAL);
    String notification = changes + "/" + _cc.getChildren(changes, false).get(0);
    Id cc = digest("cruise-control");
    assertEquals(Set.of(new ACL(ZooDefs.Perms.ALL, cc), new ACL(ZooDefs.Perms.READ, digest("broker1")),
                        new ACL(ZooDefs.Perms.READ, digest("broker2"))), acls(_cc, t1));
    assertEquals(Set.of(new ACL(ZooDefs.Perms.ALL, cc)), acls(_cc, AclZkStore.STATE_NODE));

    // broker: reads data and notifications, re-creates the folders at startup, but can't change anything
    assertNotNull(_broker.getData(t1, false, null));
    assertNotNull(_broker.getData(notification, false, null));
    for (PatternType pt : AclTree.PATTERN_TYPES) {
      for (ResourceType type : KafkaAclZkCodec.resourceTypes()) {
        createLikeBrokerStartup(_broker, KafkaAclZkCodec.typePath(pt, type));
      }
    }
    assertNoAuth("broker create resource", () -> _broker.create(KafkaAclZkCodec.dataPath(LEGACY) + "2", new byte[0],
                                                                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT));
    assertNoAuth("broker setData", () -> _broker.setData(t1, KafkaAclZkCodec.encode(List.of(WRITE_B)), -1));
    assertNoAuth("broker delete resource", () -> _broker.delete(t1, -1));
    assertNoAuth("broker delete notification", () -> _broker.delete(notification, -1));
    assertNoAuth("broker create notification", () -> _broker.create(changes + "/acl_changes_", new byte[0],
                                                                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL));
    assertNoAuth("broker setACL", () -> _broker.setACL(t1, ZooDefs.Ids.OPEN_ACL_UNSAFE, -1));
    assertNoAuth("broker reads state", () -> _broker.getData(AclZkStore.STATE_NODE, false, null));

    // other clients, authenticated or not: nothing
    for (ZooKeeper other : List.of(_mallory, _anonymous)) {
      assertNoAuth("read", () -> other.getData(t1, false, null));
      assertNoAuth("list", () -> other.getChildren("/kafka-acl/Topic", false));
      assertNoAuth("write", () -> other.setData(t1, new byte[0], -1));
      assertNoAuth("create", () -> other.create("/kafka-acl/Topic/t9", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
                                               CreateMode.PERSISTENT));
      assertNoAuth("notify", () -> other.create(changes + "/acl_changes_", "Topic:t1".getBytes(StandardCharsets.UTF_8),
                                               ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL));
      assertNoAuth("lock", () -> other.create(AclZkStore.LOCK_NODE, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
                                             CreateMode.EPHEMERAL));
    }

    // Cruise Control keeps working: update + delete, prefixed tree too
    AclTree next = tree(T1, List.of(WRITE_B));
    next.put(PG, List.of(READ_A));
    apply(next, "sha-2");
    apply(tree(PG, List.of(WRITE_B)), "sha-3");
    assertEquals(tree(PG, List.of(WRITE_B)), _store.readCurrent().tree());
  }

  @Test
  public void testPurgeNotifications() throws Exception {
    _store.ensureZnodeAcls();
    apply(tree(T1, List.of(READ_A)), "sha-1");
    AclTree withPrefixed = tree(T1, List.of(READ_A));
    withPrefixed.put(PG, List.of(READ_A));
    apply(withPrefixed, "sha-2");
    long now = System.currentTimeMillis();
    assertEquals(0, _store.purgeNotifications(60_000, now));
    // T1 added + legacy deleted (no unmanaged set here) + PG added
    assertEquals(3, _store.purgeNotifications(0, now + 1_000));
    for (PatternType pt : AclTree.PATTERN_TYPES) {
      assertEquals(List.of(), _cc.getChildren(KafkaAclZkCodec.changeRootPath(pt), false));
    }
  }

  @Test
  public void testManagerSetsZnodeAclsAndPurges() throws Exception {
    AclSyncManager.Settings settings = new AclSyncManager.Settings(60_000, 900_000, true, List.of("User", "Claim"), 1_000,
                                                                   "test", 0L);
    Path file = Files.createTempDirectory("acl-zk").resolve("acls.json");
    Files.writeString(file, AclTestFiles.oneTopic(AclTestFiles.entry("Claim:a", "Allow", "*", "Read")));
    AclSyncManager manager = new AclSyncManager(settings, new AclFileReader(file, null, null, false), () -> _store,
                                                (expected, timeoutMs) -> List.of(), AclDriftListener.LOG_ONLY, Time.SYSTEM);
    assertEquals(AclSyncManager.Outcome.APPLIED, manager.checkFile());
    // the legacy node (unmanaged) got the protected ACLs too
    assertEquals(3, acls(_cc, KafkaAclZkCodec.dataPath(LEGACY)).size());
    assertNoAuth("broker setData", () -> _broker.setData(KafkaAclZkCodec.dataPath(LEGACY), new byte[0], -1));
    // retention 0: the next check purges the notification it created
    Thread.sleep(5);
    assertEquals(AclSyncManager.Outcome.UNCHANGED, manager.checkFile());
    assertEquals(List.of(), _cc.getChildren(KafkaAclZkCodec.changeRootPath(PatternType.LITERAL), false));
  }

  @Test
  public void testFactoryRefusesSessionWithoutIdentity() {
    AclSyncManager.StoreFactory factory = AclSyncManager.storeFactory(
        _zkServer.connectionString() + CHROOT, null, 900_000,
        new AclSyncManager.ZnodeAclSettings("x509", List.of(new Id("x509", "O=LocalDev,CN=broker1"))));
    IOException e = assertThrows(IOException.class, factory::create);
    assertTrue(e.getMessage(), e.getMessage().contains("has no x509 identity"));
  }
}
