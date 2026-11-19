/*
 * Copyright 2026 qjwu. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config.constants;

import org.apache.kafka.common.config.ConfigDef;

import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;

/**
 * Settings of the <code>root_ca</code> endpoint: the SHA-256 thumbprints of the root CA certificates clients should pin
 * (e.g. librdkafka clients verifying the brokers by their root CA), checked against the certificate chains the brokers
 * actually send.
 */
public final class RootCaConfig {

  /**
   * <code>root.ca.thumbprints</code>
   */
  public static final String ROOT_CA_THUMBPRINTS_CONFIG = "root.ca.thumbprints";
  public static final String DEFAULT_ROOT_CA_THUMBPRINTS = "";
  public static final String ROOT_CA_THUMBPRINTS_DOC = "Comma-separated SHA-256 thumbprints (64 hex digits, colons "
      + "optional) of the root CA certificates clients should trust. During a root CA rotation list both the old and "
      + "the new root: announce the new one before any broker sends it, remove the old one after the rotation. The "
      + "root_ca endpoint publishes this list and reports whether every broker's certificate chain ends in a listed root.";

  /**
   * <code>root.ca.probe.cache.ms</code>
   */
  public static final String ROOT_CA_PROBE_CACHE_MS_CONFIG = "root.ca.probe.cache.ms";
  public static final long DEFAULT_ROOT_CA_PROBE_CACHE_MS = 60_000L;
  public static final String ROOT_CA_PROBE_CACHE_MS_DOC = "How long the certificate chains observed from the brokers "
      + "are reused before the brokers are probed again.";

  /**
   * <code>root.ca.probe.timeout.ms</code>
   */
  public static final String ROOT_CA_PROBE_TIMEOUT_MS_CONFIG = "root.ca.probe.timeout.ms";
  public static final int DEFAULT_ROOT_CA_PROBE_TIMEOUT_MS = 10_000;
  public static final String ROOT_CA_PROBE_TIMEOUT_MS_DOC = "Connect and handshake timeout when probing a broker's "
      + "certificate chain.";

  private RootCaConfig() {
  }

  /**
   * Define configs for the root_ca endpoint.
   *
   * @param configDef Config definition.
   * @return The given ConfigDef after defining the configs for the root_ca endpoint.
   */
  public static ConfigDef define(ConfigDef configDef) {
    return configDef.define(ROOT_CA_THUMBPRINTS_CONFIG,
                            ConfigDef.Type.LIST,
                            DEFAULT_ROOT_CA_THUMBPRINTS,
                            ConfigDef.Importance.MEDIUM,
                            ROOT_CA_THUMBPRINTS_DOC)
                    .define(ROOT_CA_PROBE_CACHE_MS_CONFIG,
                            ConfigDef.Type.LONG,
                            DEFAULT_ROOT_CA_PROBE_CACHE_MS,
                            atLeast(0),
                            ConfigDef.Importance.LOW,
                            ROOT_CA_PROBE_CACHE_MS_DOC)
                    .define(ROOT_CA_PROBE_TIMEOUT_MS_CONFIG,
                            ConfigDef.Type.INT,
                            DEFAULT_ROOT_CA_PROBE_TIMEOUT_MS,
                            atLeast(1),
                            ConfigDef.Importance.LOW,
                            ROOT_CA_PROBE_TIMEOUT_MS_DOC);
  }
}
