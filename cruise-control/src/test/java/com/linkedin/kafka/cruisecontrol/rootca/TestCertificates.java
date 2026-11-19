/*
 * Copyright 2026 qjwu. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.rootca;

import java.math.BigInteger;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.cert.X509Certificate;
import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.asn1.x509.KeyUsage;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;

/**
 * Test PKI: root CA -> intermediate CA -> leaf (SAN localhost / 127.0.0.1).
 */
public final class TestCertificates {
  private static final AtomicLong SERIAL = new AtomicLong(System.currentTimeMillis());
  private static final long DAY_MS = 86_400_000L;

  /** A certificate and its key pair. */
  public static final class Issued {
    private final X509Certificate _cert;
    private final KeyPair _keys;

    Issued(X509Certificate cert, KeyPair keys) {
      _cert = cert;
      _keys = keys;
    }

    public X509Certificate cert() {
      return _cert;
    }

    public KeyPair keys() {
      return _keys;
    }
  }

  private TestCertificates() {
  }

  /**
   * @return A new RSA key pair.
   * @throws Exception On error.
   */
  public static KeyPair keys() throws Exception {
    KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
    generator.initialize(2048);
    return generator.generateKeyPair();
  }

  /**
   * @param dn Subject DN.
   * @return A self-signed root CA.
   * @throws Exception On error.
   */
  public static Issued root(String dn) throws Exception {
    KeyPair keys = keys();
    return new Issued(build(new X500Name(dn), keys, new X500Name(dn), keys, true), keys);
  }

  /**
   * @param dn Subject DN.
   * @param issuer Issuing CA.
   * @param ca Whether it is a CA certificate (intermediate) or a leaf.
   * @return A certificate signed by the issuer.
   * @throws Exception On error.
   */
  public static Issued issue(String dn, Issued issuer, boolean ca) throws Exception {
    KeyPair keys = keys();
    return new Issued(build(new X500Name(dn), keys, name(issuer.cert()), issuer.keys(), ca), keys);
  }

  /**
   * @param subjectOf Certificate whose subject name is copied (e.g. to forge a certificate with the same name).
   * @param issuerOf Certificate whose subject name is written as the issuer.
   * @param signer Key pair that signs it (may differ from the real issuer's, to forge).
   * @param ca Whether it is a CA certificate.
   * @return A certificate.
   * @throws Exception On error.
   */
  public static Issued forge(X509Certificate subjectOf, X509Certificate issuerOf, KeyPair signer, boolean ca) throws Exception {
    KeyPair keys = keys();
    return new Issued(build(name(subjectOf), keys, name(issuerOf), signer, ca), keys);
  }

  /**
   * @param cert A certificate.
   * @return Its subject exactly as encoded (re-parsing an RFC 2253 string would reverse the order).
   */
  private static X500Name name(X509Certificate cert) {
    return X500Name.getInstance(cert.getSubjectX500Principal().getEncoded());
  }

  private static X509Certificate build(X500Name dn, KeyPair keys, X500Name issuerDn, KeyPair signer, boolean ca) throws Exception {
    long now = System.currentTimeMillis();
    JcaX509v3CertificateBuilder builder = new JcaX509v3CertificateBuilder(issuerDn, BigInteger.valueOf(SERIAL.incrementAndGet()),
                                                                          new Date(now - DAY_MS), new Date(now + 30 * DAY_MS),
                                                                          dn, keys.getPublic());
    builder.addExtension(Extension.basicConstraints, true, new BasicConstraints(ca));
    if (ca) {
      builder.addExtension(Extension.keyUsage, true, new KeyUsage(KeyUsage.keyCertSign | KeyUsage.cRLSign));
    } else {
      builder.addExtension(Extension.keyUsage, true, new KeyUsage(KeyUsage.digitalSignature | KeyUsage.keyEncipherment));
      builder.addExtension(Extension.subjectAlternativeName, false, new GeneralNames(new GeneralName[]{
          new GeneralName(GeneralName.dNSName, "localhost"), new GeneralName(GeneralName.iPAddress, "127.0.0.1")}));
    }
    return new JcaX509CertificateConverter().getCertificate(
        builder.build(new JcaContentSignerBuilder("SHA256withRSA").build(signer.getPrivate())));
  }
}
