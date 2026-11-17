/*
 * Copyright 2026 qjwu. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.quota;

import com.linkedin.kafka.cruisecontrol.metricsreporter.utils.CCKafkaIntegrationTestHarness;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.quota.ClientQuotaAlteration;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * {@link AdminQuotaStore} against an embedded broker: identity names (with ':' , '=' and ',') and the default user
 * entity round-trip through Kafka's quota API; client-id entries are reported as ignored and left alone.
 */
public class AdminQuotaStoreTest extends CCKafkaIntegrationTestHarness {
  private static final List<String> TYPES = List.of("User", "Claim", "Certificate");
  private AdminClient _admin;

  @Override
  protected int clusterSize() {
    return 1;
  }

  /**
   * Start ZooKeeper and a broker; admin client.
   */
  @Before
  @Override
  public void setUp() {
    super.setUp();
    _admin = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers()));
  }

  /**
   * Close the admin client, stop the broker and ZooKeeper.
   */
  @After
  @Override
  public void tearDown() {
    if (_admin != null) {
      _admin.close();
    }
    super.tearDown();
  }

  @Test
  public void testRoundTripAndOwnership() throws Exception {
    // a client-id quota that quota sync must leave alone
    ClientQuotaEntity clientId = new ClientQuotaEntity(Collections.singletonMap(ClientQuotaEntity.CLIENT_ID, "legacy-app"));
    _admin.alterClientQuotas(List.of(new ClientQuotaAlteration(clientId, List.of(new ClientQuotaAlteration.Op("producer_byte_rate", 5.0)))))
          .all().get(30, TimeUnit.SECONDS);
    AdminQuotaStore store = new AdminQuotaStore(_admin, 30_000);

    QuotaSet desired = QuotaFileParser.parse(QuotaFileParserTest.SAMPLE.getBytes(StandardCharsets.UTF_8), TYPES);
    store.apply(QuotaDiff.diff(store.read().userQuotas(), desired));
    TestUtils.waitForCondition(() -> store.read().userQuotas().equals(desired), 30_000, "quotas not stored");

    QuotaStore.Stored stored = store.read();
    assertEquals(desired, stored.userQuotas());
    assertEquals(1, stored.ignored().size());
    assertTrue(stored.ignored().get(0), stored.ignored().get(0).contains("legacy-app"));

    // a smaller file: removed identities and keys disappear, the client-id entry stays
    QuotaSet smaller = new QuotaSet(Map.of(QuotaSet.DEFAULT, Map.of("producer_byte_rate", 1024.0)));
    store.apply(QuotaDiff.diff(store.read().userQuotas(), smaller));
    TestUtils.waitForCondition(() -> store.read().userQuotas().equals(smaller), 30_000, "quotas not removed");
    assertEquals(1, store.read().ignored().size());
  }
}
