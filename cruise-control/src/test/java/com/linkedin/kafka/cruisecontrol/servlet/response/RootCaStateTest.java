/*
 * Copyright 2026 qjwu. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.response;

import com.google.gson.Gson;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.rootca.RootCaReport;
import java.util.List;
import java.util.Map;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RootCaStateTest {
  private static final String OLD = "15476b7b3769dae522a338cbe6f2f3c5b202d75ff866830e96801c326a8823bf";
  private static final String NEW = "349ca5544a7d38ad493a5c06e927010000000000000000000000000000000000";

  /**
   * JSON keys (optional ones omitted when empty) and the plain-text form.
   */
  @Test
  public void jsonAndPlaintext() {
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties());
    RootCaReport.Certificate root = new RootCaReport.Certificate("root", "CN=Local Kafka CA,O=LocalDev", "CN=Local Kafka CA,O=LocalDev",
                                                                 OLD, "2029-02-14T03:28:18Z");
    RootCaReport.Certificate leaf = new RootCaReport.Certificate("leaf", "CN=broker1,O=LocalDev", "CN=Kafka Intermediate CA,O=LocalDev",
                                                                 "2648", "2028-01-01T00:00:00Z");
    RootCaReport report = new RootCaReport(
        List.of(new RootCaReport.Thumbprint(OLD, List.of(1), root), new RootCaReport.Thumbprint(NEW, List.of(), null)),
        List.of(new RootCaReport.BrokerChain(1, "localhost:9092", List.of(leaf, root), null, null, OLD, true, null),
                new RootCaReport.BrokerChain(2, "localhost:9094", List.of(), null, null, null, false, "cannot get its certificate chain")),
        false, List.of("broker 2 (localhost:9094): cannot get its certificate chain"), 1234L);
    RootCaState state = new RootCaState(config, report);

    @SuppressWarnings("unchecked")
    Map<String, Object> json = new Gson().fromJson(state.getJsonString(), Map.class);
    assertEquals(1.0, json.get("version"));
    assertEquals(false, json.get("consistent"));
    List<Map<String, Object>> thumbprints = (List<Map<String, Object>>) json.get("rootCaThumbprints");
    assertEquals(OLD, thumbprints.get(0).get("sha256"));
    assertEquals("IN_USE", thumbprints.get(0).get("status"));
    assertEquals("CN=Local Kafka CA,O=LocalDev", thumbprints.get(0).get("subject"));
    assertEquals("NOT_IN_USE", thumbprints.get(1).get("status"));
    assertFalse(thumbprints.get(1).containsKey("subject"));
    List<Map<String, Object>> brokers = (List<Map<String, Object>>) json.get("brokers");
    assertEquals(true, brokers.get(0).get("chainComplete"));
    assertEquals(OLD, brokers.get(0).get("rootSha256"));
    assertEquals(2, ((List<?>) brokers.get(0).get("chain")).size());
    assertEquals("cannot get its certificate chain", brokers.get(1).get("error"));
    assertFalse(brokers.get(1).containsKey("untrustedReason"));
    assertEquals(1234.0, json.get("probedAtMs"));

    String text = state.getPlaintext();
    assertTrue(text, text.contains(OLD) && text.contains("IN_USE") && text.contains("NOT_IN_USE") && text.contains("Consistent: false"));
  }
}
