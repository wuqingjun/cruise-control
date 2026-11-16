/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector.notifier;

import com.linkedin.cruisecontrol.detector.Anomaly;
import com.linkedin.cruisecontrol.detector.AnomalyType;
import com.linkedin.kafka.cruisecontrol.detector.QuotaDriftAnomaly;
import com.linkedin.kafka.cruisecontrol.quota.QuotaDiff;
import com.linkedin.kafka.cruisecontrol.quota.QuotaDrift;
import com.linkedin.kafka.cruisecontrol.quota.QuotaSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.utils.MockTime;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class QuotaDriftNotifierTest {

  /** Records alerts instead of sending them. */
  private static final class RecordingNotifier extends SelfHealingNotifier {
    private final List<AnomalyType> _alerts = new ArrayList<>();

    RecordingNotifier() {
      super(new MockTime());
    }

    @Override
    public void alert(Anomaly anomaly, boolean autoFixTriggered, long selfHealingStartTime, AnomalyType anomalyType) {
      super.alert(anomaly, autoFixTriggered, selfHealingStartTime, anomalyType);
      _alerts.add(anomalyType);
    }
  }

  private static QuotaDrift drift() {
    QuotaSet stored = new QuotaSet(Map.of("Claim:payments-team", Map.of("producer_byte_rate", 1e9)));
    QuotaSet file = new QuotaSet(Map.of("Claim:payments-team", Map.of("producer_byte_rate", 50000.0)));
    return new QuotaDrift("ab12", QuotaDiff.diff(stored, file), true);
  }

  @Test
  public void testAnomaly() {
    QuotaDriftAnomaly anomaly = QuotaDriftAnomaly.of(drift(), 1234L);
    assertEquals(KafkaAnomalyType.QUOTA_DRIFT, anomaly.anomalyType());
    assertEquals(1234L, anomaly.detectionTimeMs());
    assertFalse(anomaly.fix());
    assertTrue(anomaly.stopOngoingExecution());
    String reason = anomaly.reasonSupplier().get();
    assertTrue(reason, reason.contains("QUOTA_DRIFT") && reason.contains("UPDATED Claim:payments-team") && reason.contains("re-applied"));
  }

  @Test
  public void testSelfHealingNotifierAlertsAndIgnores() {
    RecordingNotifier notifier = new RecordingNotifier();
    notifier.configure(Map.of(SelfHealingNotifier.SELF_HEALING_ENABLED_CONFIG, "true"));
    assertEquals(Boolean.FALSE, notifier.selfHealingEnabled().get(KafkaAnomalyType.QUOTA_DRIFT));
    assertEquals(AnomalyNotificationResult.Action.IGNORE, notifier.onQuotaDrift(QuotaDriftAnomaly.of(drift(), 1L)).action());
    assertEquals(List.of(KafkaAnomalyType.QUOTA_DRIFT), notifier._alerts);
  }

  @Test
  public void testDefaultNotifierMethodIgnores() {
    NoopNotifier notifier = new NoopNotifier();
    notifier.configure(Map.of());
    assertEquals(AnomalyNotificationResult.Action.IGNORE, notifier.onQuotaDrift(QuotaDriftAnomaly.of(drift(), 1L)).action());
  }
}
