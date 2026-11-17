/*
 * Copyright 2026 qjwu. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config.constants;

import org.apache.kafka.common.config.ConfigDef;

import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;

/**
 * Settings of the ACL sync feature: Kafka ACLs are managed from one deployed JSON file, which Cruise Control writes to the
 * brokers' ACL storage in ZooKeeper (data nodes + change notifications), so the brokers reload them without a restart.
 */
public final class AclSyncConfig {

  /**
   * <code>acl.sync.enabled</code>
   */
  public static final String ACL_SYNC_ENABLED_CONFIG = "acl.sync.enabled";
  public static final boolean DEFAULT_ACL_SYNC_ENABLED = false;
  public static final String ACL_SYNC_ENABLED_DOC = "Whether Cruise Control manages Kafka ACLs from the deployed ACL file "
      + "(" + "acl.sync.file" + "). When enabled, the file is the single source of truth for all ACLs in ZooKeeper.";

  /**
   * <code>acl.sync.file</code>
   */
  public static final String ACL_SYNC_FILE_CONFIG = "acl.sync.file";
  public static final String DEFAULT_ACL_SYNC_FILE = null;
  public static final String ACL_SYNC_FILE_DOC = "Path of the deployed ACL file (JSON with the two trees kafka-acl and "
      + "kafka-acl-extended, without the ZooKeeper root). Required if " + ACL_SYNC_ENABLED_CONFIG + " is true.";

  /**
   * <code>acl.sync.signature.file</code>
   */
  public static final String ACL_SYNC_SIGNATURE_FILE_CONFIG = "acl.sync.signature.file";
  public static final String DEFAULT_ACL_SYNC_SIGNATURE_FILE = null;
  public static final String ACL_SYNC_SIGNATURE_FILE_DOC = "Path of the detached signature of the ACL file: SHA256withRSA or "
      + "SHA256withECDSA over the file's exact bytes, base64-encoded (e.g. openssl dgst -sha256 -sign key.pem acls.json | base64). "
      + "If not set, the ACL file path with the suffix .sig is used.";

  /**
   * <code>acl.sync.signature.public.key.file</code>
   */
  public static final String ACL_SYNC_SIGNATURE_PUBLIC_KEY_FILE_CONFIG = "acl.sync.signature.public.key.file";
  public static final String DEFAULT_ACL_SYNC_SIGNATURE_PUBLIC_KEY_FILE = null;
  public static final String ACL_SYNC_SIGNATURE_PUBLIC_KEY_FILE_DOC = "Path of the PEM public key (-----BEGIN PUBLIC KEY-----, "
      + "RSA of at least 2048 bits or EC P-256/P-384/P-521) used to verify the ACL file's signature. Required if "
      + "acl.sync.signature.required is true.";

  /**
   * <code>acl.sync.signature.required</code>
   */
  public static final String ACL_SYNC_SIGNATURE_REQUIRED_CONFIG = "acl.sync.signature.required";
  public static final boolean DEFAULT_ACL_SYNC_SIGNATURE_REQUIRED = true;
  public static final String ACL_SYNC_SIGNATURE_REQUIRED_DOC = "Whether the ACL file must have a valid signature before it is "
      + "applied. Set to false only outside production.";

  /**
   * <code>acl.sync.check.interval.ms</code>
   */
  public static final String ACL_SYNC_CHECK_INTERVAL_MS_CONFIG = "acl.sync.check.interval.ms";
  public static final long DEFAULT_ACL_SYNC_CHECK_INTERVAL_MS = 60_000L;
  public static final String ACL_SYNC_CHECK_INTERVAL_MS_DOC = "How often the SHA-256 hash of the ACL file is compared with the hash "
      + "of the last applied version stored in ZooKeeper. A different hash starts an apply.";

  /**
   * <code>acl.sync.drift.check.interval.ms</code>
   */
  public static final String ACL_SYNC_DRIFT_CHECK_INTERVAL_MS_CONFIG = "acl.sync.drift.check.interval.ms";
  public static final long DEFAULT_ACL_SYNC_DRIFT_CHECK_INTERVAL_MS = 900_000L;
  public static final String ACL_SYNC_DRIFT_CHECK_INTERVAL_MS_DOC = "How often the ACL file is compared with the ACLs actually "
      + "stored in ZooKeeper even if the file is unchanged, to detect ACLs changed outside the file (drift).";

  /**
   * <code>acl.sync.drift.action</code>
   */
  public static final String ACL_SYNC_DRIFT_ACTION_CONFIG = "acl.sync.drift.action";
  public static final String DRIFT_ACTION_REAPPLY = "REAPPLY";
  public static final String DRIFT_ACTION_REPORT = "REPORT";
  public static final String DEFAULT_ACL_SYNC_DRIFT_ACTION = DRIFT_ACTION_REAPPLY;
  public static final String ACL_SYNC_DRIFT_ACTION_DOC = "What to do when drift is detected: " + DRIFT_ACTION_REAPPLY
      + " (report it and re-apply the file) or " + DRIFT_ACTION_REPORT + " (report it only).";

  /**
   * <code>acl.sync.max.batch.bytes</code>
   */
  public static final String ACL_SYNC_MAX_BATCH_BYTES_CONFIG = "acl.sync.max.batch.bytes";
  public static final int DEFAULT_ACL_SYNC_MAX_BATCH_BYTES = 900_000;
  public static final String ACL_SYNC_MAX_BATCH_BYTES_DOC = "Maximum size of one ZooKeeper multi (batch) request. Must stay below "
      + "ZooKeeper's jute.maxbuffer (1 MB by default); larger changes are split into several batches.";

  /**
   * <code>acl.sync.principal.types</code>
   */
  public static final String ACL_SYNC_PRINCIPAL_TYPES_CONFIG = "acl.sync.principal.types";
  public static final String DEFAULT_ACL_SYNC_PRINCIPAL_TYPES = "User,Claim,Certificate";
  public static final String ACL_SYNC_PRINCIPAL_TYPES_DOC = "Principal types allowed in the ACL file (case-sensitive), e.g. "
      + "User:alice, Claim:kafka-readers. An entry with another type is rejected.";

  /**
   * <code>acl.sync.verify.timeout.ms</code>
   */
  public static final String ACL_SYNC_VERIFY_TIMEOUT_MS_CONFIG = "acl.sync.verify.timeout.ms";
  public static final long DEFAULT_ACL_SYNC_VERIFY_TIMEOUT_MS = 30_000L;
  public static final String ACL_SYNC_VERIFY_TIMEOUT_MS_DOC = "How long to wait after an apply for every broker to report the "
      + "applied ACLs (describeAcls).";

  /**
   * <code>authorizer.zookeeper.url</code>
   */
  public static final String AUTHORIZER_ZOOKEEPER_URL_CONFIG = "authorizer.zookeeper.url";
  public static final String DEFAULT_AUTHORIZER_ZOOKEEPER_URL = null;
  public static final String AUTHORIZER_ZOOKEEPER_URL_DOC = "ZooKeeper connect string (including the chroot) where the brokers' "
      + "authorizer stores ACLs. Must equal the brokers' authorizer.zookeeper.url. If not set, zookeeper.connect is used, as "
      + "the brokers do.";

  /**
   * <code>acl.sync.zookeeper.acl.enabled</code>
   */
  public static final String ACL_SYNC_ZOOKEEPER_ACL_ENABLED_CONFIG = "acl.sync.zookeeper.acl.enabled";
  public static final boolean DEFAULT_ACL_SYNC_ZOOKEEPER_ACL_ENABLED = false;
  public static final String ACL_SYNC_ZOOKEEPER_ACL_ENABLED_DOC = "Whether Cruise Control protects the brokers' ACL storage in "
      + "ZooKeeper with znode ACLs, so that only Cruise Control can change Kafka ACLs there: Cruise Control gets all "
      + "permissions, the brokers (acl.sync.zookeeper.broker.identities) read (plus create on the top-level ACL folders), "
      + "nobody else anything. Requires an authenticated ZooKeeper connection (TLS client certificate, "
      + "acl.sync.zookeeper.auth.scheme). Existing nodes are updated.";

  /**
   * <code>acl.sync.zookeeper.auth.scheme</code>
   */
  public static final String ACL_SYNC_ZOOKEEPER_AUTH_SCHEME_CONFIG = "acl.sync.zookeeper.auth.scheme";
  public static final String DEFAULT_ACL_SYNC_ZOOKEEPER_AUTH_SCHEME = "x509";
  public static final String ACL_SYNC_ZOOKEEPER_AUTH_SCHEME_DOC = "ZooKeeper auth scheme of Cruise Control's and the brokers' "
      + "identities in the znode ACLs: x509 (TLS client certificates; the identity is the certificate subject DN).";

  /**
   * <code>acl.sync.zookeeper.broker.identities</code>
   */
  public static final String ACL_SYNC_ZOOKEEPER_BROKER_IDENTITIES_CONFIG = "acl.sync.zookeeper.broker.identities";
  public static final String DEFAULT_ACL_SYNC_ZOOKEEPER_BROKER_IDENTITIES = "";
  public static final String ACL_SYNC_ZOOKEEPER_BROKER_IDENTITIES_DOC = "The brokers' ZooKeeper identities, separated by ';' "
      + "(certificate DNs contain commas), e.g. O=LocalDev,CN=broker1;O=LocalDev,CN=broker2. Required if "
      + "acl.sync.zookeeper.acl.enabled is true.";

  /**
   * <code>acl.sync.notification.retention.ms</code>
   */
  public static final String ACL_SYNC_NOTIFICATION_RETENTION_MS_CONFIG = "acl.sync.notification.retention.ms";
  public static final long DEFAULT_ACL_SYNC_NOTIFICATION_RETENTION_MS = 600_000L;
  public static final String ACL_SYNC_NOTIFICATION_RETENTION_MS_DOC = "Cruise Control deletes the change notifications it "
      + "created after this time. Keep it below the brokers' own purge age (15 minutes), so that brokers never need to "
      + "delete them (they have no delete permission when acl.sync.zookeeper.acl.enabled is true).";

  /**
   * <code>acl.sync.verify.mode</code>
   */
  public static final String ACL_SYNC_VERIFY_MODE_CONFIG = "acl.sync.verify.mode";
  public static final String DEFAULT_ACL_SYNC_VERIFY_MODE = "PER_BROKER";
  public static final String ACL_SYNC_VERIFY_MODE_DOC = "How the brokers' ACLs are compared with ZooKeeper after an apply and "
      + "by the drift check. PER_BROKER: every live broker is asked (describeCluster, then a DescribeAcls request to each "
      + "broker); a stale or unreachable broker is reported by id. ANY_BROKER: one describeAcls through the admin client, "
      + "answered by a broker the admin client chooses.";

  private AclSyncConfig() {
  }

  /**
   * Define configs for ACL sync.
   *
   * @param configDef Config definition.
   * @return The given config definition after defining the configs for ACL sync.
   */
  public static ConfigDef define(ConfigDef configDef) {
    return configDef.define(ACL_SYNC_ENABLED_CONFIG,
                            ConfigDef.Type.BOOLEAN,
                            DEFAULT_ACL_SYNC_ENABLED,
                            ConfigDef.Importance.MEDIUM,
                            ACL_SYNC_ENABLED_DOC)
                    .define(ACL_SYNC_FILE_CONFIG,
                            ConfigDef.Type.STRING,
                            DEFAULT_ACL_SYNC_FILE,
                            ConfigDef.Importance.MEDIUM,
                            ACL_SYNC_FILE_DOC)
                    .define(ACL_SYNC_SIGNATURE_FILE_CONFIG,
                            ConfigDef.Type.STRING,
                            DEFAULT_ACL_SYNC_SIGNATURE_FILE,
                            ConfigDef.Importance.MEDIUM,
                            ACL_SYNC_SIGNATURE_FILE_DOC)
                    .define(ACL_SYNC_SIGNATURE_PUBLIC_KEY_FILE_CONFIG,
                            ConfigDef.Type.STRING,
                            DEFAULT_ACL_SYNC_SIGNATURE_PUBLIC_KEY_FILE,
                            ConfigDef.Importance.MEDIUM,
                            ACL_SYNC_SIGNATURE_PUBLIC_KEY_FILE_DOC)
                    .define(ACL_SYNC_SIGNATURE_REQUIRED_CONFIG,
                            ConfigDef.Type.BOOLEAN,
                            DEFAULT_ACL_SYNC_SIGNATURE_REQUIRED,
                            ConfigDef.Importance.MEDIUM,
                            ACL_SYNC_SIGNATURE_REQUIRED_DOC)
                    .define(ACL_SYNC_CHECK_INTERVAL_MS_CONFIG,
                            ConfigDef.Type.LONG,
                            DEFAULT_ACL_SYNC_CHECK_INTERVAL_MS,
                            atLeast(1000),
                            ConfigDef.Importance.LOW,
                            ACL_SYNC_CHECK_INTERVAL_MS_DOC)
                    .define(ACL_SYNC_DRIFT_CHECK_INTERVAL_MS_CONFIG,
                            ConfigDef.Type.LONG,
                            DEFAULT_ACL_SYNC_DRIFT_CHECK_INTERVAL_MS,
                            atLeast(1000),
                            ConfigDef.Importance.LOW,
                            ACL_SYNC_DRIFT_CHECK_INTERVAL_MS_DOC)
                    .define(ACL_SYNC_DRIFT_ACTION_CONFIG,
                            ConfigDef.Type.STRING,
                            DEFAULT_ACL_SYNC_DRIFT_ACTION,
                            ConfigDef.ValidString.in(DRIFT_ACTION_REAPPLY, DRIFT_ACTION_REPORT),
                            ConfigDef.Importance.LOW,
                            ACL_SYNC_DRIFT_ACTION_DOC)
                    .define(ACL_SYNC_MAX_BATCH_BYTES_CONFIG,
                            ConfigDef.Type.INT,
                            DEFAULT_ACL_SYNC_MAX_BATCH_BYTES,
                            atLeast(1024),
                            ConfigDef.Importance.LOW,
                            ACL_SYNC_MAX_BATCH_BYTES_DOC)
                    .define(ACL_SYNC_PRINCIPAL_TYPES_CONFIG,
                            ConfigDef.Type.LIST,
                            DEFAULT_ACL_SYNC_PRINCIPAL_TYPES,
                            ConfigDef.Importance.LOW,
                            ACL_SYNC_PRINCIPAL_TYPES_DOC)
                    .define(ACL_SYNC_VERIFY_TIMEOUT_MS_CONFIG,
                            ConfigDef.Type.LONG,
                            DEFAULT_ACL_SYNC_VERIFY_TIMEOUT_MS,
                            atLeast(0),
                            ConfigDef.Importance.LOW,
                            ACL_SYNC_VERIFY_TIMEOUT_MS_DOC)
                    .define(AUTHORIZER_ZOOKEEPER_URL_CONFIG,
                            ConfigDef.Type.STRING,
                            DEFAULT_AUTHORIZER_ZOOKEEPER_URL,
                            ConfigDef.Importance.MEDIUM,
                            AUTHORIZER_ZOOKEEPER_URL_DOC)
                    .define(ACL_SYNC_ZOOKEEPER_ACL_ENABLED_CONFIG,
                            ConfigDef.Type.BOOLEAN,
                            DEFAULT_ACL_SYNC_ZOOKEEPER_ACL_ENABLED,
                            ConfigDef.Importance.HIGH,
                            ACL_SYNC_ZOOKEEPER_ACL_ENABLED_DOC)
                    .define(ACL_SYNC_ZOOKEEPER_AUTH_SCHEME_CONFIG,
                            ConfigDef.Type.STRING,
                            DEFAULT_ACL_SYNC_ZOOKEEPER_AUTH_SCHEME,
                            ConfigDef.Importance.LOW,
                            ACL_SYNC_ZOOKEEPER_AUTH_SCHEME_DOC)
                    .define(ACL_SYNC_ZOOKEEPER_BROKER_IDENTITIES_CONFIG,
                            ConfigDef.Type.STRING,
                            DEFAULT_ACL_SYNC_ZOOKEEPER_BROKER_IDENTITIES,
                            ConfigDef.Importance.MEDIUM,
                            ACL_SYNC_ZOOKEEPER_BROKER_IDENTITIES_DOC)
                    .define(ACL_SYNC_NOTIFICATION_RETENTION_MS_CONFIG,
                            ConfigDef.Type.LONG,
                            DEFAULT_ACL_SYNC_NOTIFICATION_RETENTION_MS,
                            atLeast(1000),
                            ConfigDef.Importance.LOW,
                            ACL_SYNC_NOTIFICATION_RETENTION_MS_DOC)
                    .define(ACL_SYNC_VERIFY_MODE_CONFIG,
                            ConfigDef.Type.STRING,
                            DEFAULT_ACL_SYNC_VERIFY_MODE,
                            ConfigDef.ValidString.in("PER_BROKER", "ANY_BROKER"),
                            ConfigDef.Importance.LOW,
                            ACL_SYNC_VERIFY_MODE_DOC);
  }
}
