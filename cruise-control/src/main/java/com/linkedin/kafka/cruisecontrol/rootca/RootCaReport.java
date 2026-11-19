/*
 * Copyright 2026 qjwu. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.rootca;

import java.util.Collections;
import java.util.List;

/**
 * What the <code>root_ca</code> endpoint reports: the root CA thumbprints clients should pin (configured), and what each
 * broker actually sends.
 */
public final class RootCaReport {
  private final List<Thumbprint> _thumbprints;
  private final List<BrokerChain> _brokers;
  private final boolean _consistent;
  private final List<String> _problems;
  private final long _probedAtMs;

  /**
   * @param thumbprints The configured root CA thumbprints, in the configured order, with their use by the brokers.
   * @param brokers What each broker sends.
   * @param consistent Whether every broker sends a complete chain whose root is in the configured list.
   * @param problems Human-readable reasons why the state is not consistent (empty if consistent).
   * @param probedAtMs When the brokers were probed.
   */
  public RootCaReport(List<Thumbprint> thumbprints, List<BrokerChain> brokers, boolean consistent, List<String> problems,
                      long probedAtMs) {
    _thumbprints = Collections.unmodifiableList(thumbprints);
    _brokers = Collections.unmodifiableList(brokers);
    _consistent = consistent;
    _problems = Collections.unmodifiableList(problems);
    _probedAtMs = probedAtMs;
  }

  public List<Thumbprint> thumbprints() {
    return _thumbprints;
  }

  public List<BrokerChain> brokers() {
    return _brokers;
  }

  public boolean consistent() {
    return _consistent;
  }

  public List<String> problems() {
    return _problems;
  }

  public long probedAtMs() {
    return _probedAtMs;
  }

  /** A configured root CA thumbprint and the brokers whose chain ends in it. */
  public static final class Thumbprint {
    private final String _sha256;
    private final List<Integer> _brokerIds;
    private final Certificate _certificate;

    /**
     * @param sha256 The thumbprint.
     * @param brokerIds Brokers currently sending a chain that ends in this root.
     * @param certificate The root certificate as seen from a broker, or {@code null} if no broker sends it.
     */
    public Thumbprint(String sha256, List<Integer> brokerIds, Certificate certificate) {
      _sha256 = sha256;
      _brokerIds = Collections.unmodifiableList(brokerIds);
      _certificate = certificate;
    }

    public String sha256() {
      return _sha256;
    }

    public List<Integer> brokerIds() {
      return _brokerIds;
    }

    /** @return {@code IN_USE} if a broker sends this root, otherwise {@code NOT_IN_USE} (announced, or retired). */
    public String status() {
      return _brokerIds.isEmpty() ? "NOT_IN_USE" : "IN_USE";
    }

    public Certificate certificate() {
      return _certificate;
    }
  }

  /** The chain one broker sends. */
  public static final class BrokerChain {
    private final int _brokerId;
    private final String _endpoint;
    private final List<Certificate> _chain;
    private final String _incompleteReason;
    private final String _untrustedReason;
    private final String _rootSha256;
    private final boolean _rootListed;
    private final String _error;

    /**
     * @param brokerId Broker id.
     * @param endpoint host:port probed.
     * @param chain Certificates sent, leaf first (empty on error).
     * @param incompleteReason {@code null} if the chain is complete (leaf -> intermediate(s) -> self-signed root).
     * @param untrustedReason {@code null} if Cruise Control's own truststore validates the chain.
     * @param rootSha256 Thumbprint of the self-signed root at the end of the chain, {@code null} if there is none.
     * @param rootListed Whether that root is in the configured list.
     * @param error {@code null}, or why the broker couldn't be probed.
     */
    public BrokerChain(int brokerId, String endpoint, List<Certificate> chain, String incompleteReason, String untrustedReason,
                       String rootSha256, boolean rootListed, String error) {
      _brokerId = brokerId;
      _endpoint = endpoint;
      _chain = Collections.unmodifiableList(chain);
      _incompleteReason = incompleteReason;
      _untrustedReason = untrustedReason;
      _rootSha256 = rootSha256;
      _rootListed = rootListed;
      _error = error;
    }

    public int brokerId() {
      return _brokerId;
    }

    public String endpoint() {
      return _endpoint;
    }

    public List<Certificate> chain() {
      return _chain;
    }

    public boolean complete() {
      return _error == null && _incompleteReason == null;
    }

    public String incompleteReason() {
      return _incompleteReason;
    }

    public boolean trustedByCruiseControl() {
      return _error == null && _untrustedReason == null;
    }

    public String untrustedReason() {
      return _untrustedReason;
    }

    public String rootSha256() {
      return _rootSha256;
    }

    public boolean rootListed() {
      return _rootListed;
    }

    public String error() {
      return _error;
    }
  }

  /** One certificate of a chain. */
  public static final class Certificate {
    private final String _role;
    private final String _subject;
    private final String _issuer;
    private final String _sha256;
    private final String _notAfter;

    /**
     * @param role {@code leaf}, {@code intermediate} or {@code root}.
     * @param subject Subject DN.
     * @param issuer Issuer DN.
     * @param sha256 Thumbprint.
     * @param notAfter End of validity, ISO-8601 UTC.
     */
    public Certificate(String role, String subject, String issuer, String sha256, String notAfter) {
      _role = role;
      _subject = subject;
      _issuer = issuer;
      _sha256 = sha256;
      _notAfter = notAfter;
    }

    public String role() {
      return _role;
    }

    public String subject() {
      return _subject;
    }

    public String issuer() {
      return _issuer;
    }

    public String sha256() {
      return _sha256;
    }

    public String notAfter() {
      return _notAfter;
    }
  }
}
