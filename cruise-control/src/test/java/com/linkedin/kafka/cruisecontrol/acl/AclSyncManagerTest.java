/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.acl;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.acl.AclSyncManager.Outcome;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.AclSyncConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.ExecutorConfig;
import com.linkedin.kafka.cruisecontrol.metricsreporter.utils.CCEmbeddedZookeeper;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.Signature;
import java.security.spec.ECGenParameterSpec;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Deque;
import java.util.List;
import java.util.Properties;
import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.utils.Time;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * {@link AclSyncManager} against an embedded ZooKeeper (chroot {@code /kafka}), with signed files in a temp folder and a
 * fake broker verifier.
 */
public class AclSyncManagerTest {
  private static final String CHROOT = "/kafka";
  private static final int TIMEOUT_MS = 30_000;
  private static final List<String> TYPES = List.of("User", "Claim", "Certificate");

  @Rule
  public TemporaryFolder _folder = new TemporaryFolder();

  private CCEmbeddedZookeeper _zkServer;
  private ZooKeeper _zk;
  private KeyPair _keys;
  private Path _aclFile;
  private FakeVerifier _verifier;
  private List<AclDrift> _drifts;
  private AtomicInteger _storesCreated;
  private List<AclZkStore> _stores;

  /** Verifier returning queued answers (default: brokers match). */
  private static final class FakeVerifier implements AclSyncVerifier {
    private final Deque<List<ResourcePattern>> _answers = new ArrayDeque<>();
    private final List<AclTree> _expected = new ArrayList<>();

    @Override
    public List<ResourcePattern> verify(AclTree expected, long timeoutMs) {
      _expected.add(expected);
      return _answers.isEmpty() ? List.of() : _answers.poll();
    }
  }

  /**
   * Start ZooKeeper, create the broker folder structure and a signing key.
   *
   * @throws Exception On setup errors.
   */
  @Before
  public void setUp() throws Exception {
    _zkServer = new CCEmbeddedZookeeper();
    ZooKeeper rootZk = AclZkStore.connect(_zkServer.connectionString(), null, TIMEOUT_MS, TIMEOUT_MS);
    rootZk.create(CHROOT, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    rootZk.close();
    _zk = AclZkStore.connect(_zkServer.connectionString() + CHROOT, null, TIMEOUT_MS, TIMEOUT_MS);
    createBrokerStructure();
    KeyPairGenerator generator = KeyPairGenerator.getInstance("EC");
    generator.initialize(new ECGenParameterSpec("secp256r1"));
    _keys = generator.generateKeyPair();
    _aclFile = _folder.getRoot().toPath().resolve("acls.json");
    _verifier = new FakeVerifier();
    _drifts = new ArrayList<>();
    _storesCreated = new AtomicInteger();
    _stores = new ArrayList<>();
  }

  /**
   * Stop ZooKeeper.
   *
   * @throws Exception On errors.
   */
  @After
  public void tearDown() throws Exception {
    _stores.forEach(AclZkStore::close);
    if (_zk != null) {
      _zk.close();
    }
    if (_zkServer != null) {
      _zkServer.close();
    }
  }

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

  private AclSyncManager manager(boolean reapply) {
    AclSyncManager.Settings settings = new AclSyncManager.Settings(60_000, 900_000, reapply, TYPES, 1_000, "test-instance");
    AclFileReader reader = new AclFileReader(_aclFile, null, _keys.getPublic(), true);
    AclSyncManager.StoreFactory factory = () -> {
      _storesCreated.incrementAndGet();
      AclZkStore store = new AclZkStore(AclZkStore.connect(_zkServer.connectionString() + CHROOT, null, TIMEOUT_MS, TIMEOUT_MS),
                                        900_000);
      _stores.add(store);
      return store;
    };
    return new AclSyncManager(settings, reader, factory, _verifier, _drifts::add, Time.SYSTEM);
  }

  private static ResourcePattern topic(String name) {
    return new ResourcePattern(ResourceType.TOPIC, name, PatternType.LITERAL);
  }

  private static AclEntryKey read(String user) {
    return new AclEntryKey("User:" + user, AclOperation.READ, AclPermissionType.ALLOW, "*");
  }

  /**
   * @param topicsAndUsers Pairs (topic, user): the user may read the topic.
   * @return ACL file JSON.
   */
  private static String file(String... topicsAndUsers) {
    StringJoiner topics = new StringJoiner(", ");
    for (int i = 0; i < topicsAndUsers.length; i += 2) {
      topics.add(String.format("\"%s\": {\"version\": 1, \"acls\": [%s]}", topicsAndUsers[i],
                               AclTestFiles.entry("User:" + topicsAndUsers[i + 1], "Allow", "*", "Read")));
    }
    return "{\"kafka-acl\": {\"Topic\": {" + topics + "}}}";
  }

  private void deploy(String json) throws Exception {
    byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
    Files.write(_aclFile, bytes);
    Signature signer = Signature.getInstance("SHA256withECDSA");
    signer.initSign(_keys.getPrivate());
    signer.update(bytes);
    Files.writeString(_aclFile.resolveSibling("acls.json.sig"), Base64.getEncoder().encodeToString(signer.sign()));
  }

  private AclTree zkTree() throws Exception {
    return new AclZkStore(_zk, 900_000).readCurrent().tree();
  }

  private AclStateNode zkState() throws Exception {
    return new AclZkStore(_zk, 900_000).readState().orElseThrow().state();
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

  private void writeLikeABroker(ResourcePattern pattern, List<AclEntryKey> entries) throws Exception {
    _zk.create(KafkaAclZkCodec.dataPath(pattern), KafkaAclZkCodec.encode(entries), ZooDefs.Ids.OPEN_ACL_UNSAFE,
               CreateMode.PERSISTENT);
  }

  private int notificationCount() throws Exception {
    return _zk.getChildren(KafkaAclZkCodec.changeRootPath(PatternType.LITERAL), false).size();
  }

  @Test
  public void testNoFileAppliesNothing() throws Exception {
    AclSyncManager manager = manager(true);
    assertEquals(Outcome.NO_FILE, manager.checkFile());
    assertEquals(Outcome.NO_FILE, manager.checkDrift());
    assertEquals(0, zkTree().numResources());
    assertNull(_zk.exists(AclZkStore.STATE_NODE, false));
  }

  @Test
  public void testApplyThenUnchanged() throws Exception {
    deploy(file("orders", "alice", "payments", "bob"));
    AclSyncManager manager = manager(true);

    assertEquals(Outcome.APPLIED, manager.checkFile());
    assertEquals(tree(topic("orders"), List.of(read("alice")), topic("payments"), List.of(read("bob"))), zkTree());
    assertEquals(AclFileReader.sha256(Files.readAllBytes(_aclFile)), zkState().sha256());
    // the verifier was asked for what ZooKeeper has now
    assertEquals(zkTree(), _verifier._expected.get(0));
    assertEquals(2, notificationCount());
    assertNull(manager.lastError());

    assertEquals(Outcome.UNCHANGED, manager.checkFile());
    assertEquals(2, notificationCount());
    // the lock is released
    assertNull(_zk.exists(AclZkStore.LOCK_NODE, false));
  }

  @Test
  public void testChangedFileUpdatesAndDeletes() throws Exception {
    deploy(file("orders", "alice", "payments", "bob"));
    AclSyncManager manager = manager(true);
    assertEquals(Outcome.APPLIED, manager.checkFile());

    deploy(file("orders", "erin"));
    assertEquals(Outcome.APPLIED, manager.checkFile());
    assertEquals(tree(topic("orders"), List.of(read("erin"))), zkTree());
    assertEquals(4, notificationCount());
  }

  @Test
  public void testFirstRunKeepsUnmanagedAndOverridesExisting() throws Exception {
    writeLikeABroker(topic("legacy"), List.of(read("dave")));
    writeLikeABroker(topic("orders"), List.of(read("dave")));
    deploy(file("orders", "alice"));
    AclSyncManager manager = manager(true);

    assertEquals(Outcome.APPLIED, manager.checkFile());
    assertEquals(tree(topic("legacy"), List.of(read("dave")), topic("orders"), List.of(read("alice"))), zkTree());
    assertEquals(List.of(topic("legacy")), List.copyOf(zkState().unmanaged()));

    // later applies leave it alone ...
    deploy(file("orders", "bob"));
    assertEquals(Outcome.APPLIED, manager.checkFile());
    assertTrue(zkTree().contains(topic("legacy")));
    assertEquals(List.of(topic("legacy")), List.copyOf(zkState().unmanaged()));

    // ... until the file defines it: then the file manages it
    deploy(file("orders", "bob", "legacy", "erin"));
    assertEquals(Outcome.APPLIED, manager.checkFile());
    assertEquals(List.of(), List.copyOf(zkState().unmanaged()));
    deploy(file("orders", "bob"));
    assertEquals(Outcome.APPLIED, manager.checkFile());
    assertEquals(tree(topic("orders"), List.of(read("bob"))), zkTree());
  }

  @Test
  public void testInvalidFilesAreNotApplied() throws Exception {
    deploy(file("orders", "alice"));
    AclSyncManager manager = manager(true);
    assertEquals(Outcome.APPLIED, manager.checkFile());
    AclTree applied = zkTree();
    String sha = zkState().sha256();

    // content changed after signing
    deploy(file("orders", "bob"));
    Files.writeString(_aclFile, file("orders", "mallory"));
    assertEquals(Outcome.INVALID_FILE, manager.checkFile());
    assertTrue(manager.lastError(), manager.lastError().contains("doesn't match"));

    // signed, but invalid content (principal type not allowed)
    deploy("{\"kafka-acl\": {\"Topic\": {\"orders\": {\"version\": 1, \"acls\": ["
           + AclTestFiles.entry("Group:admins", "Allow", "*", "Read") + "]}}}}");
    assertEquals(Outcome.INVALID_FILE, manager.checkFile());
    assertTrue(manager.lastError(), manager.lastError().contains("principal type 'Group' is not allowed"));

    // signed, broken JSON; then the signature is removed
    deploy("{\"kafka-acl\": ");
    assertEquals(Outcome.INVALID_FILE, manager.checkFile());
    Files.delete(_aclFile.resolveSibling("acls.json.sig"));
    assertEquals(Outcome.INVALID_FILE, manager.checkFile());

    // file removed
    Files.delete(_aclFile);
    assertEquals(Outcome.NO_FILE, manager.checkFile());

    assertEquals(applied, zkTree());
    assertEquals(sha, zkState().sha256());
  }

  @Test
  public void testRejectedFileIsCheckedAgain() throws Exception {
    deploy(file("orders", "alice"));
    Path signature = _aclFile.resolveSibling("acls.json.sig");
    byte[] goodSignature = Files.readAllBytes(signature);
    Files.delete(signature);
    AclSyncManager manager = manager(true);

    // the same rejected file, again and again (logged once, but still checked every time)
    assertEquals(Outcome.INVALID_FILE, manager.checkFile());
    assertEquals(Outcome.INVALID_FILE, manager.checkFile());
    assertEquals(0, zkTree().numResources());

    // the signature arrives later: same file hash, now accepted
    Files.write(signature, goodSignature);
    assertEquals(Outcome.APPLIED, manager.checkFile());
    assertEquals(tree(topic("orders"), List.of(read("alice"))), zkTree());
    assertNull(manager.lastError());
  }

  @Test
  public void testWrongRootAndLock() throws Exception {
    deploy(file("orders", "alice"));
    _zk.delete(KafkaAclZkCodec.changeRootPath(PatternType.LITERAL), -1);
    AclSyncManager manager = manager(true);
    assertEquals(Outcome.WRONG_ROOT, manager.checkFile());
    assertTrue(manager.lastError(), manager.lastError().contains("kafka-acl-changes"));
    createBrokerStructure();

    // another instance holds the lock
    AclZkStore other = new AclZkStore(AclZkStore.connect(_zkServer.connectionString() + CHROOT, null, TIMEOUT_MS, TIMEOUT_MS),
                                      900_000);
    _stores.add(other);
    assertTrue(other.tryLock("other-instance"));
    assertEquals(Outcome.LOCKED, manager.checkFile());
    assertEquals(0, zkTree().numResources());
    other.close();
    assertEquals(Outcome.APPLIED, manager.checkFile());
  }

  @Test
  public void testVerifyFailure() throws Exception {
    deploy(file("orders", "alice"));
    AclSyncManager manager = manager(true);
    _verifier._answers.add(List.of(topic("orders")));
    assertEquals(Outcome.VERIFY_FAILED, manager.checkFile());
    assertTrue(manager.lastError(), manager.lastError().contains("kafka-acl/Topic/orders"));
    // ZooKeeper has the file: the next file check has nothing to do, the drift check looks at the brokers again
    assertEquals(Outcome.UNCHANGED, manager.checkFile());
    assertEquals(Outcome.IN_SYNC, manager.checkDrift());
  }

  @Test
  public void testDriftInZooKeeper() throws Exception {
    deploy(file("orders", "alice"));
    AclSyncManager reporter = manager(false);
    assertEquals(Outcome.APPLIED, reporter.checkFile());
    assertEquals(Outcome.IN_SYNC, reporter.checkDrift());

    // someone adds a resource and changes another directly in ZooKeeper
    writeLikeABroker(topic("rogue"), List.of(read("mallory")));
    _zk.setData(KafkaAclZkCodec.dataPath(topic("orders")), KafkaAclZkCodec.encode(List.of(read("mallory"))), -1);

    assertEquals(Outcome.DRIFT_REPORTED, reporter.checkDrift());
    assertEquals(1, _drifts.size());
    assertEquals(2, _drifts.get(0).zkChanges().size());
    assertTrue(zkTree().contains(topic("rogue")));

    AclSyncManager repairer = manager(true);
    assertEquals(Outcome.DRIFT_REAPPLIED, repairer.checkDrift());
    assertTrue(_drifts.get(1).reapplied());
    assertEquals(tree(topic("orders"), List.of(read("alice"))), zkTree());
    assertEquals(Outcome.IN_SYNC, repairer.checkDrift());
  }

  @Test
  public void testDriftOnlyInBrokersResendsNotifications() throws Exception {
    deploy(file("orders", "alice", "payments", "bob"));
    AclSyncManager manager = manager(true);
    assertEquals(Outcome.APPLIED, manager.checkFile());
    int before = notificationCount();

    // ZooKeeper matches the file, but the brokers serve something else for 'payments'
    _verifier._answers.add(List.of(topic("payments")));
    assertEquals(Outcome.DRIFT_REAPPLIED, manager.checkDrift());
    assertEquals(List.of(topic("payments")), _drifts.get(0).brokerDifferences());
    assertEquals(before + 1, notificationCount());
    String sha = zkState().sha256();
    assertEquals(AclFileReader.sha256(Files.readAllBytes(_aclFile)), sha);
  }

  @Test
  public void testDriftCheckAppliesAChangedFile() throws Exception {
    deploy(file("orders", "alice"));
    AclSyncManager manager = manager(true);
    assertEquals(Outcome.APPLIED, manager.checkDrift());
    assertEquals(tree(topic("orders"), List.of(read("alice"))), zkTree());
    assertEquals(0, _drifts.size());
  }

  @Test
  public void testReconnectsAfterSessionLoss() throws Exception {
    deploy(file("orders", "alice"));
    AclSyncManager manager = manager(true);
    assertEquals(Outcome.APPLIED, manager.checkFile());
    assertEquals(1, _storesCreated.get());

    _stores.get(0).close();
    deploy(file("orders", "bob"));
    assertEquals(Outcome.APPLIED, manager.checkFile());
    assertEquals(2, _storesCreated.get());
    assertEquals(tree(topic("orders"), List.of(read("bob"))), zkTree());
  }

  @Test
  public void testCreateFromConfig() throws Exception {
    Properties props = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    props.setProperty(AclSyncConfig.ACL_SYNC_ENABLED_CONFIG, "true");
    props.setProperty(ExecutorConfig.ZOOKEEPER_CONNECT_CONFIG, "zk1:2181/kafka");

    // no file
    assertThrows(ConfigException.class, () -> create(props));
    props.setProperty(AclSyncConfig.ACL_SYNC_FILE_CONFIG, _aclFile.toString());
    // signature required, no key
    ConfigException e = assertThrows(ConfigException.class, () -> create(props));
    assertTrue(e.getMessage(), e.getMessage().contains(AclSyncConfig.ACL_SYNC_SIGNATURE_PUBLIC_KEY_FILE_CONFIG));
    // unusable key
    Path keyFile = Files.writeString(_folder.getRoot().toPath().resolve("key.pem"), "nothing");
    props.setProperty(AclSyncConfig.ACL_SYNC_SIGNATURE_PUBLIC_KEY_FILE_CONFIG, keyFile.toString());
    assertThrows(ConfigException.class, () -> create(props));

    Files.writeString(keyFile, "-----BEGIN PUBLIC KEY-----\n" + Base64.getEncoder().encodeToString(_keys.getPublic().getEncoded())
                               + "\n-----END PUBLIC KEY-----\n");
    try (AclSyncManager manager = create(props)) {
      assertNotNull(manager);
    }

    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(props);
    assertEquals("zk1:2181/kafka", AclSyncManager.aclRoot(config));
    props.setProperty(AclSyncConfig.AUTHORIZER_ZOOKEEPER_URL_CONFIG, "zk2:2181/acls");
    assertEquals("zk2:2181/acls", AclSyncManager.aclRoot(new KafkaCruiseControlConfig(props)));
  }

  private static AclSyncManager create(Properties props) {
    return AclSyncManager.create(new KafkaCruiseControlConfig(props), null, null, AclDriftListener.LOG_ONLY, Time.SYSTEM);
  }
}
