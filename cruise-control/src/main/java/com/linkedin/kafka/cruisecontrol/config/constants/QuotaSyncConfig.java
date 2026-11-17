/*
 * Copyright 2026 qjwu. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config.constants;

import org.apache.kafka.common.config.ConfigDef;

import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;

/**
 * Settings of the quota sync feature: client quotas by security identity are managed from one deployed, signed JSON file,
 * which Cruise Control applies through Kafka's quota API (alterClientQuotas, entity type user); the brokers resolve them
 * with MultiIdentityQuotaCallback.
 */
public final class QuotaSyncConfig {

  /**
   * <code>quota.sync.enabled</code>
   */
  public static final String QUOTA_SYNC_ENABLED_CONFIG = "quota.sync.enabled";
  public static final boolean DEFAULT_QUOTA_SYNC_ENABLED = false;
  public static final String QUOTA_SYNC_ENABLED_DOC = "Whether Cruise Control applies client quotas from a "
      + "deployed, signed quota file (identity-based throttling; the brokers use MultiIdentityQuotaCallback). "
      + "The file is the source of truth for all quotas of entity type user.";

  /**
   * <code>quota.sync.file</code>
   */
  public static final String QUOTA_SYNC_FILE_CONFIG = "quota.sync.file";
  public static final String DEFAULT_QUOTA_SYNC_FILE = null;
  public static final String QUOTA_SYNC_FILE_DOC = "Path of the deployed quota file (JSON: version, default, "
      + "quotas by identity). Required if quota.sync.enabled is true.";

  /**
   * <code>quota.sync.signature.file</code>
   */
  public static final String QUOTA_SYNC_SIGNATURE_FILE_CONFIG = "quota.sync.signature.file";
  public static final String DEFAULT_QUOTA_SYNC_SIGNATURE_FILE = null;
  public static final String QUOTA_SYNC_SIGNATURE_FILE_DOC = "Path of the detached signature of the quota file "
      + "(SHA256withRSA or SHA256withECDSA over the exact bytes, base64). If not set, the quota file path "
      + "with the suffix .sig is used.";

  /**
   * <code>quota.sync.signature.public.key.file</code>
   */
  public static final String QUOTA_SYNC_SIGNATURE_PUBLIC_KEY_FILE_CONFIG = "quota.sync.signature.public.key.file";
  public static final String DEFAULT_QUOTA_SYNC_SIGNATURE_PUBLIC_KEY_FILE = null;
  public static final String QUOTA_SYNC_SIGNATURE_PUBLIC_KEY_FILE_DOC = "Path of the PEM public key used to verify the quota file's "
      + "signature. Required if quota.sync.signature.required is true.";

  /**
   * <code>quota.sync.signature.required</code>
   */
  public static final String QUOTA_SYNC_SIGNATURE_REQUIRED_CONFIG = "quota.sync.signature.required";
  public static final boolean DEFAULT_QUOTA_SYNC_SIGNATURE_REQUIRED = true;
  public static final String QUOTA_SYNC_SIGNATURE_REQUIRED_DOC = "Whether the quota file must have a valid signature before it "
      + "is applied. Set to false only outside production.";

  /**
   * <code>quota.sync.check.interval.ms</code>
   */
  public static final String QUOTA_SYNC_CHECK_INTERVAL_MS_CONFIG = "quota.sync.check.interval.ms";
  public static final long DEFAULT_QUOTA_SYNC_CHECK_INTERVAL_MS = 60_000L;
  public static final String QUOTA_SYNC_CHECK_INTERVAL_MS_DOC = "How often the deployed quota file is checked for changes.";

  /**
   * <code>quota.sync.drift.check.interval.ms</code>
   */
  public static final String QUOTA_SYNC_DRIFT_CHECK_INTERVAL_MS_CONFIG = "quota.sync.drift.check.interval.ms";
  public static final long DEFAULT_QUOTA_SYNC_DRIFT_CHECK_INTERVAL_MS = 900_000L;
  public static final String QUOTA_SYNC_DRIFT_CHECK_INTERVAL_MS_DOC = "How often the stored quotas are compared with the quota file "
      + "even when the file is unchanged. How often the stored quotas are compared with the quota file even "
      + "when the file is unchanged.";

  /**
   * <code>quota.sync.drift.action</code>
   */
  public static final String QUOTA_SYNC_DRIFT_ACTION_CONFIG = "quota.sync.drift.action";
  public static final String DEFAULT_QUOTA_SYNC_DRIFT_ACTION = "REAPPLY";
  public static final String QUOTA_SYNC_DRIFT_ACTION_DOC = "What to do when the stored quotas differ from the file: "
      + "REAPPLY (report and apply the file again) or REPORT (report only).";

  /**
   * <code>quota.sync.principal.types</code>
   */
  public static final String QUOTA_SYNC_PRINCIPAL_TYPES_CONFIG = "quota.sync.principal.types";
  public static final String DEFAULT_QUOTA_SYNC_PRINCIPAL_TYPES = "User,Claim,Certificate";
  public static final String QUOTA_SYNC_PRINCIPAL_TYPES_DOC = "Identity types allowed as keys in the quota file "
      + "(Type:name).";

  private QuotaSyncConfig() {
  }

  /**
   * Define configs for quota sync.
   *
   * @param configDef Config definition.
   * @return The given config definition after defining the configs for quota sync.
   */
  public static ConfigDef define(ConfigDef configDef) {
    return configDef
                    .define(QUOTA_SYNC_ENABLED_CONFIG,
                            ConfigDef.Type.BOOLEAN,
                            DEFAULT_QUOTA_SYNC_ENABLED,
                            ConfigDef.Importance.HIGH,
                            QUOTA_SYNC_ENABLED_DOC)
                    .define(QUOTA_SYNC_FILE_CONFIG,
                            ConfigDef.Type.STRING,
                            DEFAULT_QUOTA_SYNC_FILE,
                            ConfigDef.Importance.HIGH,
                            QUOTA_SYNC_FILE_DOC)
                    .define(QUOTA_SYNC_SIGNATURE_FILE_CONFIG,
                            ConfigDef.Type.STRING,
                            DEFAULT_QUOTA_SYNC_SIGNATURE_FILE,
                            ConfigDef.Importance.MEDIUM,
                            QUOTA_SYNC_SIGNATURE_FILE_DOC)
                    .define(QUOTA_SYNC_SIGNATURE_PUBLIC_KEY_FILE_CONFIG,
                            ConfigDef.Type.STRING,
                            DEFAULT_QUOTA_SYNC_SIGNATURE_PUBLIC_KEY_FILE,
                            ConfigDef.Importance.HIGH,
                            QUOTA_SYNC_SIGNATURE_PUBLIC_KEY_FILE_DOC)
                    .define(QUOTA_SYNC_SIGNATURE_REQUIRED_CONFIG,
                            ConfigDef.Type.BOOLEAN,
                            DEFAULT_QUOTA_SYNC_SIGNATURE_REQUIRED,
                            ConfigDef.Importance.HIGH,
                            QUOTA_SYNC_SIGNATURE_REQUIRED_DOC)
                    .define(QUOTA_SYNC_CHECK_INTERVAL_MS_CONFIG,
                            ConfigDef.Type.LONG,
                            DEFAULT_QUOTA_SYNC_CHECK_INTERVAL_MS,
                            atLeast(1000),
                            ConfigDef.Importance.MEDIUM,
                            QUOTA_SYNC_CHECK_INTERVAL_MS_DOC)
                    .define(QUOTA_SYNC_DRIFT_CHECK_INTERVAL_MS_CONFIG,
                            ConfigDef.Type.LONG,
                            DEFAULT_QUOTA_SYNC_DRIFT_CHECK_INTERVAL_MS,
                            atLeast(1000),
                            ConfigDef.Importance.MEDIUM,
                            QUOTA_SYNC_DRIFT_CHECK_INTERVAL_MS_DOC)
                    .define(QUOTA_SYNC_DRIFT_ACTION_CONFIG,
                            ConfigDef.Type.STRING,
                            DEFAULT_QUOTA_SYNC_DRIFT_ACTION,
                            ConfigDef.ValidString.in("REAPPLY", "REPORT"),
                            ConfigDef.Importance.MEDIUM,
                            QUOTA_SYNC_DRIFT_ACTION_DOC)
                    .define(QUOTA_SYNC_PRINCIPAL_TYPES_CONFIG,
                            ConfigDef.Type.LIST,
                            DEFAULT_QUOTA_SYNC_PRINCIPAL_TYPES,
                            ConfigDef.Importance.LOW,
                            QUOTA_SYNC_PRINCIPAL_TYPES_DOC);
  }
}
