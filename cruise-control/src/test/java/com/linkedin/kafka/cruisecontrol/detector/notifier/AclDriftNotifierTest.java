/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector.notifier;

import com.linkedin.cruisecontrol.detector.Anomaly;
import com.linkedin.cruisecontrol.detector.AnomalyType;
import com.linkedin.kafka.cruisecontrol.acl.AclDrift;
import com.linkedin.kafka.cruisecontrol.acl.AclEntryKey;
import com.linkedin.kafka.cruisecontrol.acl.AclTree;
import com.linkedin.kafka.cruisecontrol.acl.AclTreeDiff;
import com.linkedin.kafka.cruisecontrol.detector.AclDriftAnomaly;
import com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.utils.MockTime;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class AclDriftNotifierTest {
  private static final ResourcePattern ORDERS = new ResourcePattern(ResourceType.TOPIC, "orders", PatternType.LITERAL);
  private static final ResourcePattern PAYMENTS = new ResourcePattern(ResourceType.TOPIC, "payments", PatternType.LITERAL);

  /** Records alerts instead of sending them. */
  private static final class RecordingNotifier extends SelfHealingNotifier {
    private final List<AnomalyType> _alerts = new ArrayList<>();
    private final List<Boolean> _autoFix = new ArrayList<>();

    RecordingNotifier() {
      super(new MockTime());
    }

    @Override
    public void alert(Anomaly anomaly, boolean autoFixTriggered, long selfHealingStartTime, AnomalyType anomalyType) {
      super.alert(anomaly, autoFixTriggered, selfHealingStartTime, anomalyType);
      _alerts.add(anomalyType);
      _autoFix.add(autoFixTriggered);
    }
  }

  private static AclDrift drift() {
    AclTree current = new AclTree();
    current.put(ORDERS, List.of(new AclEntryKey("User:mallory", AclOperation.READ, AclPermissionType.ALLOW, "*")));
    AclTree desired = new AclTree();
    desired.put(ORDERS, List.of(new AclEntryKey("User:alice", AclOperation.READ, AclPermissionType.ALLOW, "*")));
    return new AclDrift("ab12", AclTreeDiff.diff(current, desired, Set.of()).changes(), List.of(PAYMENTS), true);
  }

  @Test
  public void testAnomaly() {
    AclDriftAnomaly anomaly = AclDriftAnomaly.of(drift(), 1234L);
    assertEquals(KafkaAnomalyType.ACL_DRIFT, anomaly.anomalyType());
    assertEquals(1234L, anomaly.detectionTimeMs());
    assertNotNull(anomaly.anomalyId());
    assertFalse(anomaly.fix());
    // lets the handler notify while a rebalance runs; nothing is stopped because fix() does nothing
    assertTrue(anomaly.stopOngoingExecution());
    String reason = anomaly.reasonSupplier().get();
    assertTrue(reason, reason.contains("ACL_DRIFT") && reason.contains("UPDATED kafka-acl/Topic/orders")
                       && reason.contains("kafka-acl/Topic/payments") && reason.contains("re-applied"));
    assertThrows(IllegalArgumentException.class,
                 () -> new AclDriftAnomaly().configure(Map.of(AnomalyDetectorUtils.ANOMALY_DETECTION_TIME_MS_OBJECT_CONFIG, 1L)));
  }

  @Test
  public void testSelfHealingNotifierAlertsAndIgnores() {
    RecordingNotifier notifier = new RecordingNotifier();
    // even with self-healing enabled for everything, ACL drift is never self-healed
    notifier.configure(Map.of(SelfHealingNotifier.SELF_HEALING_ENABLED_CONFIG, "true"));
    assertEquals(Boolean.FALSE, notifier.selfHealingEnabled().get(KafkaAnomalyType.ACL_DRIFT));
    assertEquals(Boolean.TRUE, notifier.selfHealingEnabled().get(KafkaAnomalyType.TOPIC_ANOMALY));

    AnomalyNotificationResult result = notifier.onAclDrift(AclDriftAnomaly.of(drift(), 1L));

    assertEquals(AnomalyNotificationResult.Action.IGNORE, result.action());
    assertEquals(List.of(KafkaAnomalyType.ACL_DRIFT), notifier._alerts);
    assertEquals(List.of(false), notifier._autoFix);
    assertNotNull(notifier.selfHealingEnabledRatio().get(KafkaAnomalyType.ACL_DRIFT));
  }

  @Test
  public void testDefaultNotifierMethodIgnores() {
    NoopNotifier notifier = new NoopNotifier();
    notifier.configure(Map.of());
    assertEquals(AnomalyNotificationResult.Action.IGNORE, notifier.onAclDrift(AclDriftAnomaly.of(drift(), 1L)).action());
    assertEquals(Boolean.FALSE, notifier.selfHealingEnabled().get(KafkaAnomalyType.ACL_DRIFT));
  }
}
