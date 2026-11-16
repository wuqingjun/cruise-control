/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.acl;

import com.linkedin.kafka.cruisecontrol.metricsreporter.utils.CCKafkaIntegrationTestHarness;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.Node;
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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * {@link PerBrokerAclSyncVerifier} against two embedded brokers. Broker 1 is misconfigured with its own ACL root
 * ({@code authorizer.zookeeper.url} → another chroot), so it never sees what Cruise Control writes: the per-broker check
 * must name broker 1 and only broker 1.
 */
public class PerBrokerAclSyncVerifierTest extends CCKafkaIntegrationTestHarness {
  private static final int TIMEOUT_MS = 30_000;
  private static final String OTHER_ROOT = "/other-acl-root";
  private static final int STALE_BROKER = 1;
  private static final ResourcePattern ORDERS = new ResourcePattern(ResourceType.TOPIC, "orders", PatternType.LITERAL);
  private static final AclEntryKey ALICE_READ = new AclEntryKey("User:alice", AclOperation.READ, AclPermissionType.ALLOW, "*");

  private AdminClient _admin;
  private final List<AutoCloseable> _closeables = new ArrayList<>();

  @Override
  protected int clusterSize() {
    return 2;
  }

  @Override
  protected Map<Object, Object> createBrokerConfig(int brokerId) {
    Map<Object, Object> config = super.createBrokerConfig(brokerId);
    config.put("authorizer.class.name", "kafka.security.authorizer.AclAuthorizer");
    config.put("allow.everyone.if.no.acl.found", "true");
    if (brokerId == STALE_BROKER) {
      createChroot(OTHER_ROOT);
      config.put("authorizer.zookeeper.url", zkConnect() + OTHER_ROOT);
    }
    return config;
  }

  private void createChroot(String chroot) {
    try {
      ZooKeeper zk = AclZkStore.connect(zkConnect(), null, TIMEOUT_MS, TIMEOUT_MS);
      try {
        zk.create(chroot, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      } catch (KeeperException.NodeExistsException e) {
        // already there
      } finally {
        zk.close();
      }
    } catch (Exception e) {
      throw new IllegalStateException("Can't create " + chroot, e);
    }
  }

  /**
   * Start ZooKeeper and the two brokers; admin client.
   */
  @Before
  @Override
  public void setUp() {
    super.setUp();
    _admin = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers()));
  }

  /**
   * Close clients, stop brokers and ZooKeeper.
   *
   * @throws Exception On errors.
   */
  @After
  @Override
  public void tearDown() {
    for (AutoCloseable c : _closeables) {
      try {
        c.close();
      } catch (Exception e) {
        // ignore
      }
    }
    if (_admin != null) {
      _admin.close();
    }
    super.tearDown();
  }

  private AclZkStore store(String root) throws IOException {
    AclZkStore store = new AclZkStore(AclZkStore.connect(zkConnect() + root, null, TIMEOUT_MS, TIMEOUT_MS), 900_000);
    _closeables.add(store);
    return store;
  }

  private PerBrokerAclSyncVerifier verifier() {
    PerBrokerAclSyncVerifier verifier = new PerBrokerAclSyncVerifier(_admin, Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                                                                                    bootstrapServers()), Time.SYSTEM);
    _closeables.add(verifier);
    return verifier;
  }

  private static void apply(AclZkStore store, AclTree desired) throws Exception {
    AclZkStore.Snapshot snapshot = store.readCurrent();
    AclTreeDiff.Result diff = AclTreeDiff.diff(snapshot.tree(), desired, Set.of());
    Optional<Integer> stateVersion = store.readState().map(AclZkStore.VersionedState::version);
    store.apply(diff.changes(), snapshot, new AclStateNode("sha", 1L, "acls.json", List.of()), stateVersion);
  }

  private static AclTree ordersTree() {
    AclTree tree = new AclTree();
    tree.put(ORDERS, List.of(ALICE_READ));
    return tree;
  }

  @Test
  public void testNamesTheBrokerOnAnotherAclRoot() throws Exception {
    AclZkStore store = store("");
    PerBrokerAclSyncVerifier verifier = verifier();
    // nothing written yet: both brokers have no ACLs, like ZooKeeper
    assertEquals(AclVerifyResult.ok(), verifier.verify(store.readCurrent().tree(), 5_000));

    apply(store, ordersTree());
    AclVerifyResult result = verifier.verify(store.readCurrent().tree(), 5_000);

    assertEquals(Map.of(STALE_BROKER, List.of(ORDERS)), result.differencesByBroker());
    assertEquals(Map.of(), result.unreachableBrokers());
    assertTrue(result.describe(), result.describe().contains("broker 1: [kafka-acl/Topic/orders]"));

    // the same ACLs under broker 1's root: now every broker matches
    AclZkStore other = store(OTHER_ROOT);
    apply(other, ordersTree());
    assertEquals(AclVerifyResult.ok(), verifier.verify(store.readCurrent().tree(), 15_000));
  }

  @Test
  public void testReportsUnreachableBroker() throws Exception {
    AclZkStore store = store("");
    Node broker0 = _admin.describeCluster().nodes().get().stream().filter(n -> n.id() == 0).findFirst().orElseThrow();
    int freePort;
    try (ServerSocket socket = new ServerSocket(0)) {
      freePort = socket.getLocalPort();
    }
    Node gone = new Node(7, "localhost", freePort);
    PerBrokerAclSyncVerifier verifier = new PerBrokerAclSyncVerifier(timeoutMs -> List.of(broker0, gone),
                                                                     Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                                                                            bootstrapServers()), Time.SYSTEM);
    _closeables.add(verifier);

    AclVerifyResult result = verifier.verify(store.readCurrent().tree(), 4_000);

    assertEquals(Map.of(), result.differencesByBroker());
    assertEquals(Set.of(7), result.unreachableBrokers().keySet());
    assertTrue(result.describe(), result.describe().startsWith("broker 7 unreachable"));
  }
}
