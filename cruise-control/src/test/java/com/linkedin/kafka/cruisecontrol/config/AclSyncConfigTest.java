/*
 * Copyright 2026 qjwu. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.config.constants.AclSyncConfig;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.common.config.ConfigException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class AclSyncConfigTest {

  @Test
  public void testDefaults() {
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties());
    assertFalse(config.getBoolean(AclSyncConfig.ACL_SYNC_ENABLED_CONFIG));
    assertNull(config.getString(AclSyncConfig.ACL_SYNC_FILE_CONFIG));
    assertTrue(config.getBoolean(AclSyncConfig.ACL_SYNC_SIGNATURE_REQUIRED_CONFIG));
    assertEquals(60_000L, (long) config.getLong(AclSyncConfig.ACL_SYNC_CHECK_INTERVAL_MS_CONFIG));
    assertEquals(900_000L, (long) config.getLong(AclSyncConfig.ACL_SYNC_DRIFT_CHECK_INTERVAL_MS_CONFIG));
    assertEquals(AclSyncConfig.DRIFT_ACTION_REAPPLY, config.getString(AclSyncConfig.ACL_SYNC_DRIFT_ACTION_CONFIG));
    assertEquals(900_000, (int) config.getInt(AclSyncConfig.ACL_SYNC_MAX_BATCH_BYTES_CONFIG));
    assertEquals(List.of("User", "Claim", "Certificate"), config.getList(AclSyncConfig.ACL_SYNC_PRINCIPAL_TYPES_CONFIG));
    assertNull(config.getString(AclSyncConfig.AUTHORIZER_ZOOKEEPER_URL_CONFIG));
  }

  @Test
  public void testValues() {
    Properties props = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    props.setProperty(AclSyncConfig.ACL_SYNC_ENABLED_CONFIG, "true");
    props.setProperty(AclSyncConfig.ACL_SYNC_FILE_CONFIG, "/opt/kafka-acls/acls.json");
    props.setProperty(AclSyncConfig.ACL_SYNC_DRIFT_ACTION_CONFIG, AclSyncConfig.DRIFT_ACTION_REPORT);
    props.setProperty(AclSyncConfig.AUTHORIZER_ZOOKEEPER_URL_CONFIG, "zk-acl:2181/acl-store");
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(props);
    assertTrue(config.getBoolean(AclSyncConfig.ACL_SYNC_ENABLED_CONFIG));
    assertEquals("/opt/kafka-acls/acls.json", config.getString(AclSyncConfig.ACL_SYNC_FILE_CONFIG));
    assertEquals(AclSyncConfig.DRIFT_ACTION_REPORT, config.getString(AclSyncConfig.ACL_SYNC_DRIFT_ACTION_CONFIG));
    assertEquals("zk-acl:2181/acl-store", config.getString(AclSyncConfig.AUTHORIZER_ZOOKEEPER_URL_CONFIG));
  }

  @Test
  public void testInvalidValuesRejected() {
    Properties action = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    action.setProperty(AclSyncConfig.ACL_SYNC_DRIFT_ACTION_CONFIG, "DELETE");
    assertThrows(ConfigException.class, () -> new KafkaCruiseControlConfig(action));

    Properties interval = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    interval.setProperty(AclSyncConfig.ACL_SYNC_CHECK_INTERVAL_MS_CONFIG, "10");
    assertThrows(ConfigException.class, () -> new KafkaCruiseControlConfig(interval));
  }
}
