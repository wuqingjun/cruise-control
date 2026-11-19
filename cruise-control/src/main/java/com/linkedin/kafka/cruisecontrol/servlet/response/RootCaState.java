/*
 * Copyright 2026 qjwu. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.response;

import com.google.gson.Gson;
import com.linkedin.cruisecontrol.servlet.parameters.CruiseControlParameters;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.rootca.RootCaReport;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.linkedin.kafka.cruisecontrol.servlet.response.ResponseUtils.JSON_VERSION;
import static com.linkedin.kafka.cruisecontrol.servlet.response.ResponseUtils.VERSION;

/**
 * Response of the <code>root_ca</code> endpoint: the root CA thumbprints clients should pin, and what each broker sends.
 */
@JsonResponseClass
public class RootCaState extends AbstractCruiseControlResponse {
  @JsonResponseField
  public static final String ROOT_CA_THUMBPRINTS = "rootCaThumbprints";
  @JsonResponseField
  public static final String CONSISTENT = "consistent";
  @JsonResponseField
  public static final String PROBLEMS = "problems";
  @JsonResponseField
  public static final String BROKERS = "brokers";
  @JsonResponseField
  public static final String PROBED_AT_MS = "probedAtMs";

  protected final RootCaReport _report;

  /**
   * @param config Cruise Control config.
   * @param report The report to return.
   */
  public RootCaState(KafkaCruiseControlConfig config, RootCaReport report) {
    super(config);
    _report = report;
  }

  /**
   * @return The JSON structure of the response (without the version).
   */
  public Map<String, Object> getJsonStructure() {
    Map<String, Object> json = new LinkedHashMap<>();
    List<Map<String, Object>> thumbprints = new ArrayList<>();
    _report.thumbprints().forEach(t -> thumbprints.add(RootCaThumbprintInfo.json(t)));
    json.put(ROOT_CA_THUMBPRINTS, thumbprints);
    json.put(CONSISTENT, _report.consistent());
    json.put(PROBLEMS, _report.problems());
    List<Map<String, Object>> brokers = new ArrayList<>();
    _report.brokers().forEach(b -> brokers.add(RootCaBrokerChain.json(b)));
    json.put(BROKERS, brokers);
    json.put(PROBED_AT_MS, _report.probedAtMs());
    return json;
  }

  protected String getJsonString() {
    Map<String, Object> json = getJsonStructure();
    json.put(VERSION, JSON_VERSION);
    return new Gson().toJson(json);
  }

  protected String getPlaintext() {
    StringBuilder sb = new StringBuilder();
    sb.append("Root CA thumbprints to pin (SHA-256):").append(System.lineSeparator());
    if (_report.thumbprints().isEmpty()) {
      sb.append("  (none configured)").append(System.lineSeparator());
    }
    for (RootCaReport.Thumbprint t : _report.thumbprints()) {
      sb.append(String.format("  %s  %-10s brokers=%s%s%n", t.sha256(), t.status(), t.brokerIds(),
                              t.certificate() == null ? "" : "  " + t.certificate().subject() + " (until " + t.certificate().notAfter() + ")"));
    }
    sb.append(String.format("Consistent: %s%n", _report.consistent()));
    for (String problem : _report.problems()) {
      sb.append("  problem: ").append(problem).append(System.lineSeparator());
    }
    sb.append("Brokers:").append(System.lineSeparator());
    for (RootCaReport.BrokerChain b : _report.brokers()) {
      sb.append(String.format("  broker %d %s: %s%n", b.brokerId(), b.endpoint(),
                              b.error() != null ? b.error() : "chain complete=" + b.complete() + ", root listed=" + b.rootListed()
                                                              + ", trusted by Cruise Control=" + b.trustedByCruiseControl()));
      for (RootCaReport.Certificate c : b.chain()) {
        sb.append(String.format("    %-12s %s  sha256=%s  until %s%n", c.role(), c.subject(), c.sha256(), c.notAfter()));
      }
    }
    return sb.toString();
  }

  @Override
  protected void discardIrrelevantAndCacheRelevant(CruiseControlParameters parameters) {
    _cachedResponse = parameters.json() ? getJsonString() : getPlaintext();
  }

  /** A configured thumbprint. */
  @JsonResponseClass
  public static final class RootCaThumbprintInfo {
    @JsonResponseField
    public static final String SHA256 = "sha256";
    @JsonResponseField
    public static final String STATUS = "status";
    @JsonResponseField
    public static final String BROKER_IDS = "brokerIds";
    @JsonResponseField(required = false)
    public static final String SUBJECT = "subject";
    @JsonResponseField(required = false)
    public static final String NOT_AFTER = "notAfter";

    private RootCaThumbprintInfo() {
    }

    static Map<String, Object> json(RootCaReport.Thumbprint t) {
      Map<String, Object> json = new LinkedHashMap<>();
      json.put(SHA256, t.sha256());
      json.put(STATUS, t.status());
      json.put(BROKER_IDS, t.brokerIds());
      if (t.certificate() != null) {
        json.put(SUBJECT, t.certificate().subject());
        json.put(NOT_AFTER, t.certificate().notAfter());
      }
      return json;
    }
  }

  /** What one broker sends. */
  @JsonResponseClass
  public static final class RootCaBrokerChain {
    @JsonResponseField
    public static final String BROKER_ID = "brokerId";
    @JsonResponseField
    public static final String ENDPOINT = "endpoint";
    @JsonResponseField
    public static final String CHAIN = "chain";
    @JsonResponseField
    public static final String CHAIN_COMPLETE = "chainComplete";
    @JsonResponseField
    public static final String ROOT_LISTED = "rootListed";
    @JsonResponseField
    public static final String TRUSTED_BY_CRUISE_CONTROL = "trustedByCruiseControl";
    @JsonResponseField(required = false)
    public static final String ROOT_SHA256 = "rootSha256";
    @JsonResponseField(required = false)
    public static final String INCOMPLETE_REASON = "incompleteReason";
    @JsonResponseField(required = false)
    public static final String UNTRUSTED_REASON = "untrustedReason";
    @JsonResponseField(required = false)
    public static final String ERROR = "error";

    private RootCaBrokerChain() {
    }

    static Map<String, Object> json(RootCaReport.BrokerChain b) {
      Map<String, Object> json = new LinkedHashMap<>();
      json.put(BROKER_ID, b.brokerId());
      json.put(ENDPOINT, b.endpoint());
      List<Map<String, Object>> chain = new ArrayList<>();
      b.chain().forEach(c -> chain.add(RootCaCertificate.json(c)));
      json.put(CHAIN, chain);
      json.put(CHAIN_COMPLETE, b.complete());
      json.put(ROOT_LISTED, b.rootListed());
      json.put(TRUSTED_BY_CRUISE_CONTROL, b.trustedByCruiseControl());
      putIfNotNull(json, ROOT_SHA256, b.rootSha256());
      putIfNotNull(json, INCOMPLETE_REASON, b.incompleteReason());
      if (b.error() == null) {
        putIfNotNull(json, UNTRUSTED_REASON, b.untrustedReason());
      }
      putIfNotNull(json, ERROR, b.error());
      return json;
    }
  }

  /** One certificate. */
  @JsonResponseClass
  public static final class RootCaCertificate {
    @JsonResponseField
    public static final String ROLE = "role";
    @JsonResponseField
    public static final String SUBJECT = "subject";
    @JsonResponseField
    public static final String ISSUER = "issuer";
    @JsonResponseField
    public static final String SHA256 = "sha256";
    @JsonResponseField
    public static final String NOT_AFTER = "notAfter";

    private RootCaCertificate() {
    }

    static Map<String, Object> json(RootCaReport.Certificate c) {
      Map<String, Object> json = new LinkedHashMap<>();
      json.put(ROLE, c.role());
      json.put(SUBJECT, c.subject());
      json.put(ISSUER, c.issuer());
      json.put(SHA256, c.sha256());
      json.put(NOT_AFTER, c.notAfter());
      return json;
    }
  }

  private static void putIfNotNull(Map<String, Object> json, String key, Object value) {
    if (value != null) {
      json.put(key, value);
    }
  }
}
