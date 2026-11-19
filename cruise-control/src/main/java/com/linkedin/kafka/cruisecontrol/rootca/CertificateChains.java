/*
 * Copyright 2026 qjwu. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.rootca;

import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Helpers for root CA thumbprints and certificate chains.
 */
public final class CertificateChains {

  private CertificateChains() {
  }

  /**
   * @param certificate A certificate.
   * @return SHA-256 of the certificate's DER encoding, 64 lowercase hex digits.
   */
  public static String sha256(X509Certificate certificate) {
    try {
      byte[] digest = MessageDigest.getInstance("SHA-256").digest(certificate.getEncoded());
      StringBuilder sb = new StringBuilder(digest.length * 2);
      for (byte b : digest) {
        sb.append(Character.forDigit((b >> 4) & 0xF, 16)).append(Character.forDigit(b & 0xF, 16));
      }
      return sb.toString();
    } catch (NoSuchAlgorithmException | CertificateEncodingException e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * Normalize thumbprints: colons / spaces removed, lowercase, duplicates removed, order kept.
   *
   * @param thumbprints Thumbprints as configured.
   * @return Normalized thumbprints.
   * @throws IllegalArgumentException If a thumbprint is not 64 hex digits.
   */
  public static List<String> normalizeThumbprints(List<String> thumbprints) {
    Set<String> result = new LinkedHashSet<>();
    for (String thumbprint : thumbprints) {
      String t = thumbprint.replace(":", "").replace(" ", "").trim().toLowerCase(Locale.ROOT);
      if (t.isEmpty()) {
        continue;
      }
      if (!t.matches("[0-9a-f]{64}")) {
        throw new IllegalArgumentException("Invalid root CA thumbprint '" + thumbprint + "': expected 64 hex digits (SHA-256).");
      }
      result.add(t);
    }
    return new ArrayList<>(result);
  }

  /**
   * @param certificate A certificate.
   * @return Whether it is self-signed (subject = issuer and its signature verifies with its own key).
   */
  public static boolean isSelfSigned(X509Certificate certificate) {
    if (!certificate.getSubjectX500Principal().equals(certificate.getIssuerX500Principal())) {
      return false;
    }
    try {
      certificate.verify(certificate.getPublicKey());
      return true;
    } catch (GeneralSecurityException e) {
      return false;
    }
  }

  /**
   * A chain is complete if it has at least two certificates, each certificate is issued and signed by the next one, and
   * the last one is a self-signed root.
   *
   * @param chain Certificates as sent by the broker, leaf first.
   * @return {@code null} if the chain is complete, otherwise the reason.
   */
  public static String incompleteReason(List<X509Certificate> chain) {
    if (chain.size() < 2) {
      return "chain has " + chain.size() + " certificate(s); expected leaf -> intermediate(s) -> root";
    }
    for (int i = 0; i + 1 < chain.size(); i++) {
      X509Certificate cert = chain.get(i);
      X509Certificate issuer = chain.get(i + 1);
      if (!cert.getIssuerX500Principal().equals(issuer.getSubjectX500Principal())) {
        return "certificate [" + i + "] " + subject(cert) + " is not issued by the next certificate " + subject(issuer);
      }
      try {
        cert.verify(issuer.getPublicKey());
      } catch (GeneralSecurityException e) {
        return "signature of certificate [" + i + "] " + subject(cert) + " does not verify with the key of " + subject(issuer);
      }
    }
    X509Certificate last = chain.get(chain.size() - 1);
    if (!isSelfSigned(last)) {
      return "last certificate " + subject(last) + " is not a self-signed root CA";
    }
    return null;
  }

  /**
   * @param certificate A certificate.
   * @return Its subject, RFC 2253.
   */
  public static String subject(X509Certificate certificate) {
    return certificate.getSubjectX500Principal().getName();
  }
}
