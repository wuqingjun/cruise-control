/*
 * Copyright 2026 qjwu. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.quota;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.acl.AclFileReader;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.QuotaSyncConfig;
import com.linkedin.kafka.cruisecontrol.quota.QuotaSyncManager.Outcome;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.Signature;
import java.security.spec.ECGenParameterSpec;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Time;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * {@link QuotaSyncManager} with a fake quota store (what Kafka would hold) and signed files in a temp folder.
 */
public class QuotaSyncManagerTest {
  private static final List<String> TYPES = List.of("User", "Claim", "Certificate");

  @Rule
  public TemporaryFolder _folder = new TemporaryFolder();

  private KeyPair _keys;
  private Path _file;
  private FakeStore _store;
  private List<QuotaDrift> _drifts;

  /** In-memory stand-in for Kafka's quota storage. */
  private static final class FakeStore implements QuotaStore {
    private final Map<String, Map<String, Double>> _userQuotas = new HashMap<>();
    private final List<String> _ignored = new ArrayList<>();
    private int _applies;
    private RuntimeException _failure;

    @Override
    public Stored read() {
      if (_failure != null) {
        throw _failure;
      }
      return new Stored(new QuotaSet(_userQuotas), _ignored);
    }

    @Override
    public void apply(List<QuotaDiff.Change> changes) {
      if (_failure != null) {
        throw _failure;
      }
      _applies++;
      for (QuotaDiff.Change change : changes) {
        Map<String, Double> values = _userQuotas.computeIfAbsent(change.identity(), i -> new HashMap<>());
        values.putAll(change.set());
        change.remove().forEach(values::remove);
        if (values.isEmpty()) {
          _userQuotas.remove(change.identity());
        }
      }
    }

    QuotaSet quotas() {
      return new QuotaSet(_userQuotas);
    }
  }

  /**
   * Signing key, file location, fake store.
   *
   * @throws Exception On setup errors.
   */
  @Before
  public void setUp() throws Exception {
    KeyPairGenerator generator = KeyPairGenerator.getInstance("EC");
    generator.initialize(new ECGenParameterSpec("secp256r1"));
    _keys = generator.generateKeyPair();
    _file = _folder.getRoot().toPath().resolve("quotas.json");
    _store = new FakeStore();
    _drifts = new ArrayList<>();
  }

  private QuotaSyncManager manager(boolean reapply) {
    return new QuotaSyncManager(new QuotaSyncManager.Settings(60_000, 900_000, reapply, TYPES),
                                new AclFileReader(_file, null, _keys.getPublic(), true), _store, _drifts::add, Time.SYSTEM);
  }

  private void deploy(String json) throws Exception {
    byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
    Files.write(_file, bytes);
    Signature signer = Signature.getInstance("SHA256withECDSA");
    signer.initSign(_keys.getPrivate());
    signer.update(bytes);
    Files.writeString(_file.resolveSibling("quotas.json.sig"), Base64.getEncoder().encodeToString(signer.sign()));
  }

  private static String file(String quotas) {
    return "{\"version\": 1, \"default\": {\"producer_byte_rate\": 100}, \"quotas\": {" + quotas + "}}";
  }

  @Test
  public void testNoFile() {
    QuotaSyncManager manager = manager(true);
    assertEquals(Outcome.NO_FILE, manager.checkFile());
    assertEquals(0, _store._applies);
  }

  @Test
  public void testApplyThenUnchanged() throws Exception {
    _store._userQuotas.put("alice", new HashMap<>(Map.of("producer_byte_rate", 5.0)));
    deploy(QuotaFileParserTest.SAMPLE);
    QuotaSyncManager manager = manager(true);

    assertEquals(Outcome.APPLIED, manager.checkFile());
    // the file owns all user quotas: old 'alice' is gone (decision C3)
    assertEquals(QuotaFileParser.parse(QuotaFileParserTest.SAMPLE.getBytes(StandardCharsets.UTF_8), TYPES), _store.quotas());
    assertNull(manager.lastError());

    assertEquals(Outcome.UNCHANGED, manager.checkFile());
    assertEquals(1, _store._applies);
  }

  @Test
  public void testChangedFile() throws Exception {
    deploy(file("\"Claim:a\": {\"producer_byte_rate\": 10}, \"Claim:b\": {\"consumer_byte_rate\": 20}"));
    QuotaSyncManager manager = manager(true);
    assertEquals(Outcome.APPLIED, manager.checkFile());

    deploy(file("\"Claim:a\": {\"producer_byte_rate\": 30}"));
    assertEquals(Outcome.APPLIED, manager.checkFile());
    assertEquals(new QuotaSet(Map.of(QuotaSet.DEFAULT, Map.of("producer_byte_rate", 100.0),
                                     "Claim:a", Map.of("producer_byte_rate", 30.0))), _store.quotas());
  }

  @Test
  public void testInvalidFilesAreNotApplied() throws Exception {
    deploy(file("\"Claim:a\": {\"producer_byte_rate\": 10}"));
    QuotaSyncManager manager = manager(true);
    assertEquals(Outcome.APPLIED, manager.checkFile());
    QuotaSet applied = _store.quotas();

    // tampered after signing
    deploy(file("\"Claim:a\": {\"producer_byte_rate\": 20}"));
    Files.writeString(_file, file("\"Claim:a\": {\"producer_byte_rate\": 99999}"));
    assertEquals(Outcome.INVALID_FILE, manager.checkFile());
    assertTrue(manager.lastError(), manager.lastError().contains("doesn't match"));
    // signed but invalid
    deploy(file("\"Group:x\": {\"producer_byte_rate\": 10}"));
    assertEquals(Outcome.INVALID_FILE, manager.checkFile());
    assertTrue(manager.lastError(), manager.lastError().contains("identity type 'Group' is not allowed"));
    // no signature
    Files.delete(_file.resolveSibling("quotas.json.sig"));
    assertEquals(Outcome.INVALID_FILE, manager.checkFile());
    // removed
    Files.delete(_file);
    assertEquals(Outcome.NO_FILE, manager.checkFile());

    assertEquals(applied, _store.quotas());
  }

  @Test
  public void testDriftReportedOrRepaired() throws Exception {
    deploy(file("\"Claim:a\": {\"producer_byte_rate\": 10}"));
    QuotaSyncManager reporter = manager(false);
    assertEquals(Outcome.APPLIED, reporter.checkFile());
    assertEquals(Outcome.IN_SYNC, reporter.checkDrift());

    // someone allowed to changes quotas out of band
    _store._userQuotas.get("Claim:a").put("producer_byte_rate", 1e9);
    _store._userQuotas.put("Claim:rogue", new HashMap<>(Map.of("consumer_byte_rate", 1e9)));
    assertEquals(Outcome.DRIFT_REPORTED, reporter.checkDrift());
    assertEquals(1, _drifts.size());
    assertEquals(2, _drifts.get(0).changes().size());
    assertEquals(1e9, _store._userQuotas.get("Claim:a").get("producer_byte_rate"), 0);

    QuotaSyncManager repairer = manager(true);
    assertEquals(Outcome.APPLIED, repairer.checkFile());
    _store._userQuotas.get("Claim:a").put("producer_byte_rate", 1e9);
    assertEquals(Outcome.DRIFT_REAPPLIED, repairer.checkDrift());
    assertTrue(_drifts.get(1).reapplied());
    assertEquals(new QuotaSet(Map.of(QuotaSet.DEFAULT, Map.of("producer_byte_rate", 100.0),
                                     "Claim:a", Map.of("producer_byte_rate", 10.0))), _store.quotas());
  }

  /**
   * While a newly deployed file is invalid, drift against the last valid file is reported (alert only), not repaired.
   *
   * @throws Exception On error.
   */
  @Test
  public void testDriftAlertedWhileTheDeployedFileIsInvalid() throws Exception {
    deploy(file("\"Claim:a\": {\"producer_byte_rate\": 10}"));
    QuotaSyncManager manager = manager(true);
    assertEquals(Outcome.APPLIED, manager.checkFile());
    // a new file that fails validation (tampered after signing)
    deploy(file("\"Claim:a\": {\"producer_byte_rate\": 20}"));
    Files.writeString(_file, file("\"Claim:a\": {\"producer_byte_rate\": 99999}"));
    // no drift: just the invalid file
    assertEquals(Outcome.INVALID_FILE, manager.checkDrift());
    assertEquals(0, _drifts.size());
    // drift while the file is invalid: alerted against the last valid file, not repaired (even with REAPPLY)
    _store._userQuotas.get("Claim:a").put("producer_byte_rate", 1e9);
    assertEquals(Outcome.INVALID_FILE, manager.checkDrift());
    assertEquals(1, _drifts.size());
    assertTrue(_drifts.get(0).whileFileInvalid());
    assertFalse(_drifts.get(0).reapplied());
    assertTrue(_drifts.get(0).toString(), _drifts.get(0).toString().contains("the deployed file is invalid"));
    assertEquals(1e9, _store._userQuotas.get("Claim:a").get("producer_byte_rate"), 0);
    // a valid file again: applied, drift gone
    deploy(file("\"Claim:a\": {\"producer_byte_rate\": 20}"));
    assertEquals(Outcome.APPLIED, manager.checkDrift());
    assertEquals(20.0, _store._userQuotas.get("Claim:a").get("producer_byte_rate"), 0);
  }

  @Test
  public void testDriftCheckAppliesAChangedFile() throws Exception {
    deploy(file("\"Claim:a\": {\"producer_byte_rate\": 10}"));
    QuotaSyncManager manager = manager(true);
    assertEquals(Outcome.APPLIED, manager.checkDrift());
    deploy(file("\"Claim:a\": {\"producer_byte_rate\": 20}"));
    assertEquals(Outcome.APPLIED, manager.checkDrift());
    assertEquals(20.0, _store._userQuotas.get("Claim:a").get("producer_byte_rate"), 0);
    assertEquals(0, _drifts.size());
  }

  @Test
  public void testStoreErrorsAreRetried() throws Exception {
    deploy(file("\"Claim:a\": {\"producer_byte_rate\": 10}"));
    QuotaSyncManager manager = manager(true);
    _store._failure = new IllegalStateException("brokers unreachable");
    assertEquals(Outcome.ERROR, manager.checkFile());
    assertTrue(manager.lastError(), manager.lastError().contains("brokers unreachable"));
    _store._failure = null;
    assertEquals(Outcome.APPLIED, manager.checkFile());
  }

  @Test
  public void testIgnoredEntriesAreLeftAlone() throws Exception {
    _store._ignored.add("ClientQuotaEntity(entries={client-id=app})");
    deploy(file(""));
    QuotaSyncManager manager = manager(true);
    assertEquals(Outcome.APPLIED, manager.checkFile());
    assertEquals(List.of("ClientQuotaEntity(entries={client-id=app})"), _store._ignored);
  }

  @Test
  public void testCreateFromConfig() throws Exception {
    Properties props = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    props.setProperty(QuotaSyncConfig.QUOTA_SYNC_ENABLED_CONFIG, "true");
    assertThrows(ConfigException.class, () -> create(props));
    props.setProperty(QuotaSyncConfig.QUOTA_SYNC_FILE_CONFIG, _file.toString());
    ConfigException e = assertThrows(ConfigException.class, () -> create(props));
    assertTrue(e.getMessage(), e.getMessage().contains(QuotaSyncConfig.QUOTA_SYNC_SIGNATURE_PUBLIC_KEY_FILE_CONFIG));
    Path keyFile = Files.writeString(_folder.getRoot().toPath().resolve("key.pem"), "-----BEGIN PUBLIC KEY-----\n"
        + Base64.getEncoder().encodeToString(_keys.getPublic().getEncoded()) + "\n-----END PUBLIC KEY-----\n");
    props.setProperty(QuotaSyncConfig.QUOTA_SYNC_SIGNATURE_PUBLIC_KEY_FILE_CONFIG, keyFile.toString());
    try (QuotaSyncManager manager = create(props)) {
      assertNotNull(manager);
    }
  }

  private static QuotaSyncManager create(Properties props) {
    return QuotaSyncManager.create(new KafkaCruiseControlConfig(props), null, null, QuotaDriftListener.LOG_ONLY, Time.SYSTEM);
  }
}
