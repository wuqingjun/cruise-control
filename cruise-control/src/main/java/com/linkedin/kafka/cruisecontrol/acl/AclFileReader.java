/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.acl;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.security.Signature;
import java.security.interfaces.ECPublicKey;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;
import java.util.Optional;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads the deployed ACL file and its detached signature, computes the SHA-256 used for change detection, and verifies
 * the signature.
 * <ul>
 *   <li>Public key: PEM {@code -----BEGIN PUBLIC KEY-----} (X.509 SubjectPublicKeyInfo); RSA of at least 2048 bits
 *       ({@code SHA256withRSA}) or EC P-256 / P-384 / P-521 ({@code SHA256withECDSA}).</li>
 *   <li>Signature: over the file's exact bytes, base64-encoded (whitespace ignored), e.g.
 *       {@code openssl dgst -sha256 -sign private.pem acls.json | base64 > acls.json.sig}.</li>
 * </ul>
 * The file and the signature are read once into memory; the hash, the signature check and the parser all use those same
 * bytes, so a file replaced in between can't be verified as one version and applied as another.
 */
public final class AclFileReader {
  /** Upper bound for the ACL file; far above any realistic file, it only protects the process from a wrong path. */
  public static final long MAX_FILE_BYTES = 64L * 1024 * 1024;
  static final long MAX_SIGNATURE_BYTES = 16L * 1024;
  static final int MIN_RSA_BITS = 2048;
  static final Set<Integer> EC_FIELD_SIZES = Set.of(256, 384, 521);
  private static final Logger LOG = LoggerFactory.getLogger(AclFileReader.class);
  private static final String PEM_BEGIN = "-----BEGIN PUBLIC KEY-----";
  private static final String PEM_END = "-----END PUBLIC KEY-----";

  private final Path _aclFile;
  private final Path _signatureFile;
  private final PublicKey _publicKey;
  private final boolean _signatureRequired;

  /**
   * The deployed file as read at one point in time.
   */
  public static final class AclFile {
    private final Path _path;
    private final byte[] _bytes;
    private final String _sha256;
    private final byte[] _signature;

    AclFile(Path path, byte[] bytes, String sha256, byte[] signature) {
      _path = path;
      _bytes = bytes;
      _sha256 = sha256;
      _signature = signature;
    }

    /**
     * @return Path of the ACL file.
     */
    public Path path() {
      return _path;
    }

    /**
     * @return The file's bytes (a copy).
     */
    public byte[] bytes() {
      return _bytes.clone();
    }

    /**
     * @return SHA-256 of the file's bytes, lowercase hex.
     */
    public String sha256() {
      return _sha256;
    }

    /**
     * @return Whether a signature file was found next to the ACL file.
     */
    public boolean hasSignature() {
      return _signature != null;
    }
  }

  /**
   * @param aclFile The deployed ACL file.
   * @param signatureFile The detached signature, or {@code null} for {@code <aclFile>.sig}.
   * @param publicKey Key to verify with (see {@link #loadPublicKey(Path)}); may be {@code null} only if the signature is
   *                  not required.
   * @param signatureRequired Whether a valid signature is required before the file is used.
   */
  public AclFileReader(Path aclFile, Path signatureFile, PublicKey publicKey, boolean signatureRequired) {
    if (aclFile == null) {
      throw new IllegalArgumentException("The ACL file path must be set.");
    }
    if (signatureRequired && publicKey == null) {
      throw new IllegalArgumentException("A public key is required when the signature is required.");
    }
    if (publicKey != null) {
      checkKey(publicKey);
    }
    _aclFile = aclFile;
    _signatureFile = signatureFile != null ? signatureFile : aclFile.resolveSibling(aclFile.getFileName() + ".sig");
    _publicKey = publicKey;
    _signatureRequired = signatureRequired;
  }

  /**
   * @return Path of the ACL file.
   */
  public Path aclFile() {
    return _aclFile;
  }

  /**
   * @return Path of the signature file used.
   */
  public Path signatureFile() {
    return _signatureFile;
  }

  /**
   * Read the ACL file and, if present, its signature, and compute the SHA-256. The signature is not checked here, so that
   * an unchanged file (same hash as last applied) costs only the hash.
   *
   * @return The file, or empty if the ACL file doesn't exist (nothing deployed yet: nothing is applied).
   * @throws AclFileException If a file exists but can't be read or is too large.
   */
  public Optional<AclFile> read() throws AclFileException {
    byte[] bytes = readLimited(_aclFile, MAX_FILE_BYTES, "ACL file");
    if (bytes == null) {
      return Optional.empty();
    }
    byte[] signature = readLimited(_signatureFile, MAX_SIGNATURE_BYTES, "signature file");
    return Optional.of(new AclFile(_aclFile, bytes, sha256(bytes), signature));
  }

  /**
   * Verify the file's signature. If the signature is not required, a missing signature or key is only logged; a present
   * signature is still checked when a key is configured.
   *
   * @param file The file from {@link #read()}.
   * @throws AclFileException If the signature is required but missing, not valid base64, or doesn't match the bytes.
   */
  public void verify(AclFile file) throws AclFileException {
    if (file._signature == null || _publicKey == null) {
      if (_signatureRequired) {
        throw new AclFileException(String.format("Signature file %s not found; the ACL file %s is not applied.", _signatureFile,
                                                 file._path));
      }
      LOG.warn("ACL file {} is used without signature verification (acl.sync.signature.required=false).", file._path);
      return;
    }
    byte[] signature;
    try {
      signature = Base64.getDecoder().decode(stripWhitespace(new String(file._signature, StandardCharsets.US_ASCII)));
    } catch (IllegalArgumentException e) {
      throw new AclFileException(String.format("Signature file %s is not valid base64.", _signatureFile), e);
    }
    boolean valid;
    try {
      Signature verifier = Signature.getInstance(signatureAlgorithm(_publicKey));
      verifier.initVerify(_publicKey);
      verifier.update(file._bytes);
      valid = verifier.verify(signature);
    } catch (GeneralSecurityException e) {
      // e.g. a malformed ECDSA DER structure
      valid = false;
    }
    if (!valid) {
      throw new AclFileException(String.format("The signature %s doesn't match the ACL file %s (sha256 %s); the file is not "
                                               + "applied.", _signatureFile, file._path, file._sha256));
    }
  }

  /**
   * Load a PEM public key ({@code -----BEGIN PUBLIC KEY-----}), RSA or EC.
   *
   * @param pemFile The key file.
   * @return The key, checked against the allowed sizes / curves.
   * @throws AclFileException If the file can't be read or holds no usable key.
   */
  public static PublicKey loadPublicKey(Path pemFile) throws AclFileException {
    String pem;
    try {
      pem = Files.readString(pemFile, StandardCharsets.US_ASCII);
    } catch (IOException e) {
      throw new AclFileException("Can't read the public key file " + pemFile, e);
    }
    int begin = pem.indexOf(PEM_BEGIN);
    int end = pem.indexOf(PEM_END);
    if (begin < 0 || end < begin) {
      throw new AclFileException(String.format("%s has no '%s' block (for a PKCS#1 'RSA PUBLIC KEY', convert it with "
                                               + "openssl rsa -RSAPublicKey_in -in key.pem -pubout).", pemFile, PEM_BEGIN));
    }
    if (pem.indexOf(PEM_BEGIN, end) >= 0) {
      throw new AclFileException(pemFile + " must contain exactly one public key.");
    }
    byte[] der;
    try {
      der = Base64.getDecoder().decode(stripWhitespace(pem.substring(begin + PEM_BEGIN.length(), end)));
    } catch (IllegalArgumentException e) {
      throw new AclFileException(pemFile + ": the public key is not valid base64.", e);
    }
    PublicKey key = decodeKey(der, pemFile);
    try {
      checkKey(key);
    } catch (IllegalArgumentException e) {
      throw new AclFileException(pemFile + ": " + e.getMessage(), e);
    }
    return key;
  }

  private static PublicKey decodeKey(byte[] der, Path pemFile) throws AclFileException {
    for (String algorithm : new String[]{"RSA", "EC"}) {
      try {
        return KeyFactory.getInstance(algorithm).generatePublic(new X509EncodedKeySpec(der));
      } catch (InvalidKeySpecException e) {
        // try the next algorithm
        LOG.trace("{} is not an {} key.", pemFile, algorithm, e);
      } catch (NoSuchAlgorithmException e) {
        throw new IllegalStateException("The JVM doesn't support " + algorithm, e);
      }
    }
    throw new AclFileException(pemFile + ": the public key is neither RSA nor EC.");
  }

  /**
   * @param key Key to check.
   * @throws IllegalArgumentException If the key type, size or curve is not allowed.
   */
  static void checkKey(PublicKey key) {
    if (key instanceof RSAPublicKey) {
      int bits = ((RSAPublicKey) key).getModulus().bitLength();
      if (bits < MIN_RSA_BITS) {
        throw new IllegalArgumentException(String.format("RSA key has %d bits; at least %d are required.", bits, MIN_RSA_BITS));
      }
    } else if (key instanceof ECPublicKey) {
      int size = ((ECPublicKey) key).getParams().getCurve().getField().getFieldSize();
      if (!EC_FIELD_SIZES.contains(size)) {
        throw new IllegalArgumentException(String.format("EC key on a %d-bit curve; P-256, P-384 or P-521 is required.", size));
      }
    } else {
      throw new IllegalArgumentException("Unsupported key algorithm " + key.getAlgorithm() + "; RSA or EC is required.");
    }
  }

  /**
   * @param key An RSA or EC key.
   * @return {@code SHA256withRSA} or {@code SHA256withECDSA}.
   */
  static String signatureAlgorithm(PublicKey key) {
    return key instanceof RSAPublicKey ? "SHA256withRSA" : "SHA256withECDSA";
  }

  /**
   * @param bytes Data.
   * @return SHA-256 of the data, lowercase hex.
   */
  static String sha256(byte[] bytes) {
    try {
      byte[] digest = MessageDigest.getInstance("SHA-256").digest(bytes);
      StringBuilder hex = new StringBuilder(digest.length * 2);
      for (byte b : digest) {
        hex.append(Character.forDigit((b >> 4) & 0xF, 16)).append(Character.forDigit(b & 0xF, 16));
      }
      return hex.toString();
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("The JVM doesn't support SHA-256", e);
    }
  }

  private static String stripWhitespace(String text) {
    return text.replaceAll("\\s", "");
  }

  /**
   * @param path File to read.
   * @param maxBytes Size limit.
   * @param what Name for error messages.
   * @return The file's bytes, or {@code null} if it doesn't exist.
   * @throws AclFileException If it can't be read or is larger than the limit.
   */
  private static byte[] readLimited(Path path, long maxBytes, String what) throws AclFileException {
    try {
      long size = Files.size(path);
      if (size > maxBytes) {
        throw new AclFileException(String.format("The %s %s has %d bytes; the limit is %d.", what, path, size, maxBytes));
      }
      byte[] bytes = Files.readAllBytes(path);
      if (bytes.length > maxBytes) {
        throw new AclFileException(String.format("The %s %s grew beyond %d bytes while it was read.", what, path, maxBytes));
      }
      return bytes;
    } catch (NoSuchFileException e) {
      return null;
    } catch (IOException e) {
      throw new AclFileException(String.format("Can't read the %s %s.", what, path), e);
    }
  }
}
