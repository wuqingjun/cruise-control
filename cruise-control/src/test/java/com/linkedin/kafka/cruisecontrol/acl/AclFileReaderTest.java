/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.acl;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Signature;
import java.security.spec.ECGenParameterSpec;
import java.util.Base64;
import java.util.Optional;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class AclFileReaderTest {
  /** Produced with LibreSSL: {@code openssl ecparam -name prime256v1 -genkey}, {@code openssl ec -pubout}. */
  private static final String OPENSSL_EC_PUBLIC_KEY = "-----BEGIN PUBLIC KEY-----\n"
      + "MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAE39am24vlj2EflERbZqMi1ZQOWGpI\n"
      + "qP0311pt9tUXVUICPwiQSBZneYI0TYtZT/vXSgO5xynesbdgcUeieKLToQ==\n"
      + "-----END PUBLIC KEY-----\n";
  private static final String OPENSSL_FILE = "{\"kafka-acl\": {\"Topic\": {\"orders\": {\"version\": 1, \"acls\": [{\"principal\": "
      + "\"User:alice@example.com\", \"permissionType\": \"Allow\", \"host\": \"*\", \"operation\": \"Read\"}]}}}}\n";
  /** {@code openssl dgst -sha256 -sign ec.pem acls.json | base64}. */
  private static final String OPENSSL_SIGNATURE =
      "MEUCIBwfehOBSTpo3tzeiK7xQLwSZSszK+9zaYMLpNzuYBuPAiEAsEplIc0osP/nMUFrrqHt+5PGUi4hXscJSs8BTtoy2RE=\n";
  /** {@code shasum -a 256 acls.json}. */
  private static final String OPENSSL_FILE_SHA256 = "4af2c9612f4b87abb0e13b048f30b2b22a4e4f66d8aee66428bd99ca4e91c28a";

  @Rule
  public TemporaryFolder _folder = new TemporaryFolder();

  private static KeyPair rsa(int bits) throws GeneralSecurityException {
    KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
    generator.initialize(bits);
    return generator.generateKeyPair();
  }

  private static KeyPair ec(String curve) throws GeneralSecurityException {
    KeyPairGenerator generator = KeyPairGenerator.getInstance("EC");
    generator.initialize(new ECGenParameterSpec(curve));
    return generator.generateKeyPair();
  }

  private static String pem(PublicKey key) {
    return "-----BEGIN PUBLIC KEY-----\n" + Base64.getMimeEncoder(64, "\n".getBytes(StandardCharsets.US_ASCII))
                                                  .encodeToString(key.getEncoded()) + "\n-----END PUBLIC KEY-----\n";
  }

  /**
   * Sign like the pipeline: signature over the exact bytes, base64 with line breaks (as {@code base64} prints it).
   *
   * @param key Private key.
   * @param data File content.
   * @return Signature file content.
   * @throws GeneralSecurityException On signing errors.
   */
  private static String sign(PrivateKey key, byte[] data) throws GeneralSecurityException {
    Signature signer = Signature.getInstance("RSA".equals(key.getAlgorithm()) ? "SHA256withRSA" : "SHA256withECDSA");
    signer.initSign(key);
    signer.update(data);
    return Base64.getMimeEncoder(76, "\n".getBytes(StandardCharsets.US_ASCII)).encodeToString(signer.sign()) + "\n";
  }

  private Path write(String name, String content) throws IOException {
    return Files.writeString(_folder.getRoot().toPath().resolve(name), content, StandardCharsets.UTF_8);
  }

  private AclFileReader reader(PublicKey key, boolean required) {
    return new AclFileReader(_folder.getRoot().toPath().resolve("acls.json"), null, key, required);
  }

  private AclFileReader.AclFile readOrFail(AclFileReader reader) throws AclFileException {
    Optional<AclFileReader.AclFile> file = reader.read();
    assertTrue(file.isPresent());
    return file.get();
  }

  private AclFileException verifyFails(AclFileReader reader) throws AclFileException {
    AclFileReader.AclFile file = readOrFail(reader);
    return assertThrows(AclFileException.class, () -> reader.verify(file));
  }

  @Test
  public void testFileSignedWithOpenssl() throws Exception {
    write("acls.json", OPENSSL_FILE);
    write("acls.json.sig", OPENSSL_SIGNATURE);
    PublicKey key = AclFileReader.loadPublicKey(write("ec.pub.pem", OPENSSL_EC_PUBLIC_KEY));
    AclFileReader reader = reader(key, true);

    AclFileReader.AclFile file = readOrFail(reader);
    reader.verify(file);

    assertEquals(OPENSSL_FILE_SHA256, file.sha256());
    assertEquals(OPENSSL_FILE, new String(file.bytes(), StandardCharsets.UTF_8));
    // the verified bytes are what the parser gets
    assertEquals(1, AclTreeParser.parse(file.bytes()).numResources());
  }

  @Test
  public void testRsaAndEcKeysVerify() throws Exception {
    byte[] content = AclTestFiles.LOCAL_CLUSTER.getBytes(StandardCharsets.UTF_8);
    write("acls.json", AclTestFiles.LOCAL_CLUSTER);
    for (KeyPair pair : new KeyPair[]{rsa(2048), rsa(4096), ec("secp256r1"), ec("secp384r1"), ec("secp521r1")}) {
      write("acls.json.sig", sign(pair.getPrivate(), content));
      PublicKey key = AclFileReader.loadPublicKey(write("key.pem", pem(pair.getPublic())));
      AclFileReader reader = reader(key, true);
      reader.verify(readOrFail(reader));
    }
  }

  @Test
  public void testModifiedFileOrWrongKeyRejected() throws Exception {
    KeyPair pair = ec("secp256r1");
    write("acls.json", AclTestFiles.LOCAL_CLUSTER);
    write("acls.json.sig", sign(pair.getPrivate(), AclTestFiles.LOCAL_CLUSTER.getBytes(StandardCharsets.UTF_8)));
    assertTrue(verifyFails(reader(ec("secp256r1").getPublic(), true)).getMessage().contains("doesn't match"));

    // one extra space is a different file
    write("acls.json", AclTestFiles.LOCAL_CLUSTER + " ");
    assertTrue(verifyFails(reader(pair.getPublic(), true)).getMessage().contains("doesn't match"));

    // an RSA signature checked with an EC key, and garbage
    write("acls.json.sig", sign(rsa(2048).getPrivate(), AclTestFiles.LOCAL_CLUSTER.getBytes(StandardCharsets.UTF_8)));
    assertTrue(verifyFails(reader(pair.getPublic(), true)).getMessage().contains("doesn't match"));
    write("acls.json.sig", Base64.getEncoder().encodeToString(new byte[]{1, 2, 3}));
    assertTrue(verifyFails(reader(pair.getPublic(), true)).getMessage().contains("doesn't match"));
  }

  @Test
  public void testSignatureNotBase64() throws Exception {
    write("acls.json", AclTestFiles.LOCAL_CLUSTER);
    write("acls.json.sig", "not base64 !");
    assertTrue(verifyFails(reader(ec("secp256r1").getPublic(), true)).getMessage().contains("not valid base64"));
  }

  @Test
  public void testMissingFiles() throws Exception {
    PublicKey key = ec("secp256r1").getPublic();
    // nothing deployed yet: nothing to apply
    assertEquals(Optional.empty(), reader(key, true).read());

    write("acls.json", AclTestFiles.LOCAL_CLUSTER);
    AclFileReader.AclFile file = readOrFail(reader(key, true));
    assertFalse(file.hasSignature());
    assertTrue(verifyFails(reader(key, true)).getMessage().contains("Signature file"));

    // not required (non-production): accepted without signature, with or without a key
    reader(key, false).verify(file);
    reader(null, false).verify(file);
  }

  @Test
  public void testSignatureCheckedWhenPresentEvenIfNotRequired() throws Exception {
    write("acls.json", AclTestFiles.LOCAL_CLUSTER);
    write("acls.json.sig", sign(ec("secp256r1").getPrivate(), new byte[]{0}));
    assertTrue(verifyFails(reader(ec("secp256r1").getPublic(), false)).getMessage().contains("doesn't match"));
  }

  @Test
  public void testSignatureSizeLimit() throws Exception {
    write("acls.json", AclTestFiles.LOCAL_CLUSTER);
    write("acls.json.sig", "A".repeat((int) AclFileReader.MAX_SIGNATURE_BYTES + 4));
    AclFileException e = assertThrows(AclFileException.class, () -> reader(ec("secp256r1").getPublic(), true).read());
    assertTrue(e.getMessage(), e.getMessage().contains("the limit is"));
  }

  @Test
  public void testSignatureFilePath() throws Exception {
    Path acl = _folder.getRoot().toPath().resolve("acls.json");
    assertEquals(_folder.getRoot().toPath().resolve("acls.json.sig"), new AclFileReader(acl, null, null, false).signatureFile());
    Path other = _folder.getRoot().toPath().resolve("signatures/acls.sig");
    assertEquals(other, new AclFileReader(acl, other, null, false).signatureFile());
    assertThrows(IllegalArgumentException.class, () -> new AclFileReader(acl, null, null, true));
  }

  @Test
  public void testPublicKeyRules() throws Exception {
    PublicKey small = rsa(1024).getPublic();
    AclFileException e = assertThrows(AclFileException.class, () -> AclFileReader.loadPublicKey(write("k.pem", pem(small))));
    assertTrue(e.getMessage(), e.getMessage().contains("at least 2048"));
    assertThrows(IllegalArgumentException.class, () -> new AclFileReader(Path.of("acls.json"), null, small, true));

    e = assertThrows(AclFileException.class, () -> AclFileReader.loadPublicKey(
        write("k.pem", "-----BEGIN RSA PUBLIC KEY-----\nAAAA\n-----END RSA PUBLIC KEY-----\n")));
    assertTrue(e.getMessage(), e.getMessage().contains("RSAPublicKey_in"));

    String one = pem(ec("secp256r1").getPublic());
    e = assertThrows(AclFileException.class, () -> AclFileReader.loadPublicKey(write("k.pem", one + one)));
    assertTrue(e.getMessage(), e.getMessage().contains("exactly one"));

    e = assertThrows(AclFileException.class, () -> AclFileReader.loadPublicKey(
        write("k.pem", "-----BEGIN PUBLIC KEY-----\nAAAA\n-----END PUBLIC KEY-----\n")));
    assertTrue(e.getMessage(), e.getMessage().contains("neither RSA nor EC"));

    e = assertThrows(AclFileException.class, () -> AclFileReader.loadPublicKey(_folder.getRoot().toPath().resolve("missing.pem")));
    assertTrue(e.getMessage(), e.getMessage().contains("Can't read"));
  }
}
