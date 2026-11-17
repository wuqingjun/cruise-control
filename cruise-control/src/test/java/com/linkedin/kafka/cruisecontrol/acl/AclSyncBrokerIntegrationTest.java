/*
 * Copyright 2026 qjwu. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.acl;

import com.linkedin.kafka.cruisecontrol.metricsreporter.utils.CCKafkaIntegrationTestHarness;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.test.TestUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * End to end: ACLs written by {@link AclZkStore} are picked up by a real (embedded) Kafka broker running
 * {@code AclAuthorizer}. The broker gets the change notification, reloads the changed resource from ZooKeeper, reports
 * the new ACLs ({@code describeAcls}) and enforces them (produce allowed / denied).
 * <p>
 * The test client connects over PLAINTEXT, so it is {@code User:ANONYMOUS}. With {@code allow.everyone.if.no.acl.found=true}
 * a resource without ACLs is open, and a resource with ACLs denies every principal that no entry allows.
 */
public class AclSyncBrokerIntegrationTest extends CCKafkaIntegrationTestHarness {
  private static final int TIMEOUT_MS = 30_000;
  private static final String ORDERS = "orders";
  private static final String PAYMENTS = "payments";
  private static final ResourcePattern ORDERS_LITERAL = new ResourcePattern(ResourceType.TOPIC, ORDERS, PatternType.LITERAL);
  private static final ResourcePattern PAYMENTS_LITERAL = new ResourcePattern(ResourceType.TOPIC, PAYMENTS, PatternType.LITERAL);
  private static final ResourcePattern ORD_PREFIXED = new ResourcePattern(ResourceType.TOPIC, "ord", PatternType.PREFIXED);
  private static final AclEntryKey ALICE_READ = new AclEntryKey("User:alice@example.com", AclOperation.READ, AclPermissionType.ALLOW, "*");
  private static final AclEntryKey BOB_READ = new AclEntryKey("User:bob@example.com", AclOperation.READ, AclPermissionType.ALLOW, "*");
  private static final AclEntryKey ANONYMOUS_WRITE = new AclEntryKey("User:ANONYMOUS", AclOperation.WRITE, AclPermissionType.ALLOW, "*");
  private static final AclEntryKey ANONYMOUS_DESCRIBE =
      new AclEntryKey("User:ANONYMOUS", AclOperation.DESCRIBE, AclPermissionType.ALLOW, "*");

  private AdminClient _admin;
  private ZooKeeper _zk;
  private AclZkStore _store;
  private int _syncs;

  @Override
  protected int clusterSize() {
    return 1;
  }

  @Override
  protected Map<Object, Object> overridingProps() {
    return Map.of("authorizer.class.name", "kafka.security.authorizer.AclAuthorizer",
                  "allow.everyone.if.no.acl.found", "true");
  }

  /**
   * Start ZooKeeper and one broker, create the topics and connect a store to the broker's ACL root.
   */
  @Before
  @Override
  public void setUp() {
    super.setUp();
    Properties props = new Properties();
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
    _admin = AdminClient.create(props);
    try {
      _admin.createTopics(List.of(new NewTopic(ORDERS, 1, (short) 1), new NewTopic(PAYMENTS, 1, (short) 1))).all().get();
      _zk = AclZkStore.connect(zkConnect(), null, TIMEOUT_MS, TIMEOUT_MS);
    } catch (Exception e) {
      throw new IllegalStateException("Setup failed", e);
    }
    _store = new AclZkStore(_zk, 900_000);
  }

  /**
   * Close the clients and stop the broker and ZooKeeper.
   */
  @After
  @Override
  public void tearDown() {
    if (_admin != null) {
      _admin.close();
    }
    if (_store != null) {
      _store.close();
    }
    super.tearDown();
  }

  @Test
  public void testBrokerReloadsLiteralTopicAclsWrittenByTheStore() throws Exception {
    // No ACLs yet: the topic is open.
    TestUtils.waitForCondition(() -> canProduce(ORDERS), TIMEOUT_MS, "produce should work before any ACL");

    // 1. Create: only alice may read 'orders' -> ANONYMOUS is denied.
    sync(tree(ORDERS_LITERAL, List.of(ALICE_READ)));
    awaitBrokerAcls(ORDERS_LITERAL, Set.of(ALICE_READ));
    TestUtils.waitForCondition(() -> !canProduce(ORDERS), TIMEOUT_MS, "produce should be denied");

    // 2. Update: add Write + Describe for ANONYMOUS -> allowed again.
    sync(tree(ORDERS_LITERAL, List.of(ALICE_READ, ANONYMOUS_WRITE, ANONYMOUS_DESCRIBE)));
    awaitBrokerAcls(ORDERS_LITERAL, Set.of(ALICE_READ, ANONYMOUS_WRITE, ANONYMOUS_DESCRIBE));
    TestUtils.waitForCondition(() -> canProduce(ORDERS), TIMEOUT_MS, "produce should be allowed");

    // 3. Delete: the resource leaves the file -> no ACLs, open again.
    sync(new AclTree());
    awaitBrokerAcls(ORDERS_LITERAL, Set.of());
    assertEquals(Set.of(), brokerAcls(ORDERS_LITERAL));
    TestUtils.waitForCondition(() -> canProduce(ORDERS), TIMEOUT_MS, "produce should be allowed without ACLs");
  }

  @Test
  public void testBrokerReloadsPrefixedTopicAclsWrittenByTheStore() throws Exception {
    TestUtils.waitForCondition(() -> canProduce(ORDERS), TIMEOUT_MS, "produce should work before any ACL");

    // A prefixed pattern goes through /kafka-acl-extended and its own change root.
    sync(tree(ORD_PREFIXED, List.of(ALICE_READ)));
    awaitBrokerAcls(ORD_PREFIXED, Set.of(ALICE_READ));
    TestUtils.waitForCondition(() -> !canProduce(ORDERS), TIMEOUT_MS, "'orders' should be denied through prefix 'ord'");
    TestUtils.waitForCondition(() -> canProduce(PAYMENTS), TIMEOUT_MS, "'payments' doesn't match the prefix");

    sync(new AclTree());
    awaitBrokerAcls(ORD_PREFIXED, Set.of());
    TestUtils.waitForCondition(() -> canProduce(ORDERS), TIMEOUT_MS, "prefix removed, 'orders' open again");
  }

  /**
   * The broker reloads only the resource named in the notification. A data node changed in ZooKeeper without a
   * notification (here 'payments') is not picked up when another resource ('orders') is changed through the store.
   *
   * @throws Exception On errors.
   */
  @Test
  public void testBrokerReloadsOnlyTheNotifiedResource() throws Exception {
    sync(tree(ORDERS_LITERAL, List.of(ALICE_READ), PAYMENTS_LITERAL, List.of(ALICE_READ)));
    awaitBrokerAcls(ORDERS_LITERAL, Set.of(ALICE_READ));
    awaitBrokerAcls(PAYMENTS_LITERAL, Set.of(ALICE_READ));

    // Change 'payments' behind the broker's back: data node only, no notification.
    _zk.setData(KafkaAclZkCodec.dataPath(PAYMENTS_LITERAL), KafkaAclZkCodec.encode(List.of(BOB_READ)), -1);

    // Change 'orders' through the store: data node + notification for Topic:orders.
    sync(tree(ORDERS_LITERAL, List.of(BOB_READ), PAYMENTS_LITERAL, List.of(BOB_READ)));
    awaitBrokerAcls(ORDERS_LITERAL, Set.of(BOB_READ));
    // The store saw ZooKeeper already matches for 'payments', so it sent no notification and the broker keeps its cache.
    assertEquals(Set.of(ALICE_READ), brokerAcls(PAYMENTS_LITERAL));

    // A notification for 'payments' (what the store sends when it changes it) makes the broker reload it.
    KafkaAclZkCodec.ChangeNode change = KafkaAclZkCodec.changeNode(PAYMENTS_LITERAL);
    _zk.create(change.pathPrefix(), change.data(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
    awaitBrokerAcls(PAYMENTS_LITERAL, Set.of(BOB_READ));
  }

  /**
   * The whole manager against the broker: file -> ZooKeeper -> broker, verified with {@link AdminAclSyncVerifier}; then a
   * data node edited without a notification is found by the drift check (brokers vs ZooKeeper) and repaired by
   * re-sending the notification.
   *
   * @throws Exception On errors.
   */
  @Test
  public void testManagerEndToEnd() throws Exception {
    Path aclFile = Files.createTempDirectory("acl-sync").resolve("acls.json");
    Files.writeString(aclFile, "{\"kafka-acl\": {\"Topic\": {\"orders\": {\"version\": 1, \"acls\": ["
        + AclTestFiles.entry("User:alice@example.com", "Allow", "*", "Read") + "]}}}}");
    AclSyncManager.Settings settings = new AclSyncManager.Settings(60_000, 900_000, true, List.of("User"), 5_000, "test");
    AclSyncManager manager = new AclSyncManager(settings, new AclFileReader(aclFile, null, null, false),
                                                () -> new AclZkStore(AclZkStore.connect(zkConnect(), null, TIMEOUT_MS, TIMEOUT_MS),
                                                                     900_000),
                                                new AdminAclSyncVerifier(_admin, Time.SYSTEM),
                                                AclDriftListener.LOG_ONLY, Time.SYSTEM);
    try {
      assertEquals(AclSyncManager.Outcome.APPLIED, manager.checkFile());
      assertEquals(Set.of(ALICE_READ), brokerAcls(ORDERS_LITERAL));
      assertEquals(AclSyncManager.Outcome.IN_SYNC, manager.checkDrift());

      // edited behind the brokers' back, then put back to the file's content: ZooKeeper == file, brokers differ
      _zk.setData(KafkaAclZkCodec.dataPath(ORDERS_LITERAL), KafkaAclZkCodec.encode(List.of(BOB_READ)), -1);
      KafkaAclZkCodec.ChangeNode change = KafkaAclZkCodec.changeNode(ORDERS_LITERAL);
      _zk.create(change.pathPrefix(), change.data(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
      awaitBrokerAcls(ORDERS_LITERAL, Set.of(BOB_READ));
      _zk.setData(KafkaAclZkCodec.dataPath(ORDERS_LITERAL), KafkaAclZkCodec.encode(List.of(ALICE_READ)), -1);
      assertEquals(Set.of(BOB_READ), brokerAcls(ORDERS_LITERAL));

      assertEquals(AclSyncManager.Outcome.DRIFT_REAPPLIED, manager.checkDrift());
      assertEquals(Set.of(ALICE_READ), brokerAcls(ORDERS_LITERAL));
    } finally {
      manager.shutdown();
    }
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

  private void sync(AclTree desired) throws Exception {
    AclZkStore.Snapshot snapshot = _store.readCurrent();
    AclTreeDiff.Result diff = AclTreeDiff.diff(snapshot.tree(), desired, Set.of());
    Optional<Integer> stateVersion = _store.readState().map(AclZkStore.VersionedState::version);
    _syncs++;
    _store.apply(diff.changes(), snapshot, new AclStateNode("sha-" + _syncs, System.currentTimeMillis(), "acls.json", List.of()),
                 stateVersion);
  }

  private Set<AclEntryKey> brokerAcls(ResourcePattern pattern) throws Exception {
    ResourcePatternFilter filter = new ResourcePatternFilter(pattern.resourceType(), pattern.name(), pattern.patternType());
    Set<AclEntryKey> entries = new HashSet<>();
    for (AclBinding binding : _admin.describeAcls(new AclBindingFilter(filter, AccessControlEntryFilter.ANY)).values().get()) {
      entries.add(AclEntryKey.of(binding.entry()));
    }
    return entries;
  }

  private void awaitBrokerAcls(ResourcePattern pattern, Set<AclEntryKey> expected) throws InterruptedException {
    TestUtils.waitForCondition(() -> expected.equals(brokerAcls(pattern)), TIMEOUT_MS,
                               () -> "broker didn't reload " + pattern + "; expected " + expected);
  }

  private boolean canProduce(String topic) {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false");
    props.put(ProducerConfig.ACKS_CONFIG, "1");
    props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "5000");
    props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
    props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "10000");
    try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props)) {
      producer.send(new ProducerRecord<>(topic, new byte[]{1})).get();
      return true;
    } catch (TopicAuthorizationException e) {
      return false;
    } catch (ExecutionException e) {
      if (e.getCause() instanceof TopicAuthorizationException) {
        return false;
      }
      throw new IllegalStateException("Unexpected produce failure", e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    }
  }
}
