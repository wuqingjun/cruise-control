/*
 * Copyright 2026 qjwu. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.rootca;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.RootCaConfig;
import java.io.IOException;
import java.security.KeyStore;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.security.ssl.DefaultSslEngineFactory;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Publishes the root CA thumbprints clients should pin ({@link RootCaConfig#ROOT_CA_THUMBPRINTS_CONFIG}) and checks them
 * against what the brokers actually send: every broker's endpoint (the listener Cruise Control uses) is probed for its
 * certificate chain; the state is consistent if every broker sends a complete chain (leaf -> intermediate(s) -> root)
 * ending in a listed root. Results are cached for {@link RootCaConfig#ROOT_CA_PROBE_CACHE_MS_CONFIG}.
 */
public class RootCaMonitor {
  private static final Logger LOG = LoggerFactory.getLogger(RootCaMonitor.class);

  /** Where the broker chains come from (a TLS probe in production). */
  @FunctionalInterface
  public interface ChainSource {
    /**
     * @param host Broker host.
     * @param port Broker port.
     * @return The chain the broker sends, leaf first.
     * @throws IOException If it can't be obtained.
     */
    List<X509Certificate> chain(String host, int port) throws IOException;
  }

  private final List<String> _thumbprints;
  private final Supplier<Collection<Node>> _brokers;
  private final ChainSource _chainSource;
  private final X509TrustManager _ccTrust;
  private final String _securityProtocol;
  private final long _cacheMs;
  private final Time _time;
  private RootCaReport _cached;

  /**
   * @param thumbprints Root CA thumbprints clients should pin (normalized).
   * @param brokers The brokers to probe.
   * @param chainSource Source of the broker chains.
   * @param ccTrust Cruise Control's own trust (its truststore), or {@code null} if unknown.
   * @param securityProtocol The protocol Cruise Control uses to connect to the brokers.
   * @param cacheMs How long a report is reused.
   * @param time Time.
   */
  public RootCaMonitor(List<String> thumbprints, Supplier<Collection<Node>> brokers, ChainSource chainSource,
                       X509TrustManager ccTrust, String securityProtocol, long cacheMs, Time time) {
    _thumbprints = List.copyOf(thumbprints);
    _brokers = brokers;
    _chainSource = chainSource;
    _ccTrust = ccTrust;
    _securityProtocol = securityProtocol;
    _cacheMs = cacheMs;
    _time = time;
  }

  /**
   * @param config Cruise Control config.
   * @param adminClient Supplier of Cruise Control's admin client (for the broker endpoints).
   * @return A monitor using Cruise Control's connection settings to the brokers.
   */
  public static RootCaMonitor create(KafkaCruiseControlConfig config, Supplier<AdminClient> adminClient) {
    List<String> thumbprints;
    try {
      thumbprints = CertificateChains.normalizeThumbprints(config.getList(RootCaConfig.ROOT_CA_THUMBPRINTS_CONFIG));
    } catch (IllegalArgumentException e) {
      throw new ConfigException(RootCaConfig.ROOT_CA_THUMBPRINTS_CONFIG + ": " + e.getMessage());
    }
    Map<String, Object> adminConfigs = KafkaCruiseControlUtils.parseAdminClientConfigs(config);
    Object protocol = adminConfigs.get(AdminClientConfig.SECURITY_PROTOCOL_CONFIG);
    String securityProtocol = protocol == null ? SecurityProtocol.PLAINTEXT.name : protocol.toString();
    int timeoutMs = config.getInt(RootCaConfig.ROOT_CA_PROBE_TIMEOUT_MS_CONFIG);
    BrokerCertificateChainProbe probe = new BrokerCertificateChainProbe(timeoutMs);
    Supplier<Collection<Node>> brokers = () -> {
      try {
        return adminClient.get().describeCluster().nodes().get(timeoutMs, TimeUnit.MILLISECONDS);
      } catch (Exception e) {
        throw new IllegalStateException("Cannot list the brokers: " + e.getMessage(), e);
      }
    };
    LOG.info("Root CA endpoint: {} configured root CA thumbprint(s) {}.", thumbprints.size(), thumbprints);
    return new RootCaMonitor(thumbprints, brokers, probe::probe, usesTls(securityProtocol) ? ccTrust(adminConfigs) : null,
                             securityProtocol, config.getLong(RootCaConfig.ROOT_CA_PROBE_CACHE_MS_CONFIG), Time.SYSTEM);
  }

  private static boolean usesTls(String securityProtocol) {
    return SecurityProtocol.SSL.name.equals(securityProtocol) || SecurityProtocol.SASL_SSL.name.equals(securityProtocol);
  }

  /**
   * Cruise Control's own trust: the truststore it uses for its admin client (JVM default trust if none is set).
   *
   * @param adminConfigs Cruise Control's admin client configs.
   * @return The trust manager, or {@code null} if the truststore can't be loaded.
   */
  private static X509TrustManager ccTrust(Map<String, Object> adminConfigs) {
    DefaultSslEngineFactory factory = new DefaultSslEngineFactory();
    try {
      factory.configure(new AdminClientConfig(adminConfigs).values());
      KeyStore truststore = factory.truststore();
      TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      tmf.init(truststore);
      for (TrustManager manager : tmf.getTrustManagers()) {
        if (manager instanceof X509TrustManager) {
          return (X509TrustManager) manager;
        }
      }
    } catch (Exception e) {
      LOG.warn("Root CA endpoint: cannot load Cruise Control's truststore; chains won't be validated against it: {}",
               e.getMessage());
    } finally {
      factory.close();
    }
    return null;
  }

  /**
   * @return The current report (probing the brokers if the cached one is older than the cache time).
   */
  public synchronized RootCaReport report() {
    long now = _time.milliseconds();
    if (_cached == null || now - _cached.probedAtMs() >= _cacheMs) {
      _cached = probe(now);
    }
    return _cached;
  }

  private RootCaReport probe(long now) {
    List<String> problems = new ArrayList<>();
    List<RootCaReport.BrokerChain> brokers = new ArrayList<>();
    if (_thumbprints.isEmpty()) {
      problems.add(RootCaConfig.ROOT_CA_THUMBPRINTS_CONFIG + " is not set: clients have no root CA to pin");
    }
    if (!usesTls(_securityProtocol)) {
      problems.add("Cruise Control connects to the brokers with security.protocol=" + _securityProtocol
                   + ": no certificate chains to observe");
    } else {
      try {
        for (Node node : _brokers.get()) {
          RootCaReport.BrokerChain chain = brokerChain(node);
          brokers.add(chain);
          describeProblems(chain, problems);
        }
      } catch (RuntimeException e) {
        problems.add(e.getMessage());
      }
      if (brokers.isEmpty() && problems.isEmpty()) {
        problems.add("no brokers found");
      }
    }
    brokers.sort((a, b) -> Integer.compare(a.brokerId(), b.brokerId()));

    List<RootCaReport.Thumbprint> thumbprints = new ArrayList<>();
    Map<String, RootCaReport.Certificate> rootsSeen = new HashMap<>();
    for (RootCaReport.BrokerChain chain : brokers) {
      if (chain.rootSha256() != null) {
        rootsSeen.putIfAbsent(chain.rootSha256(), chain.chain().get(chain.chain().size() - 1));
      }
    }
    for (String thumbprint : _thumbprints) {
      List<Integer> ids = new ArrayList<>();
      for (RootCaReport.BrokerChain chain : brokers) {
        if (thumbprint.equals(chain.rootSha256())) {
          ids.add(chain.brokerId());
        }
      }
      thumbprints.add(new RootCaReport.Thumbprint(thumbprint, ids, rootsSeen.get(thumbprint)));
    }
    boolean consistent = problems.isEmpty();
    if (!consistent) {
      LOG.warn("Root CA endpoint: not consistent: {}", problems);
    }
    return new RootCaReport(thumbprints, brokers, consistent, problems, now);
  }

  private static void describeProblems(RootCaReport.BrokerChain chain, List<String> problems) {
    String broker = "broker " + chain.brokerId() + " (" + chain.endpoint() + ")";
    if (chain.error() != null) {
      problems.add(broker + ": " + chain.error());
      return;
    }
    if (chain.incompleteReason() != null) {
      problems.add(broker + " sends an incomplete certificate chain: " + chain.incompleteReason());
    }
    if (chain.rootSha256() != null && !chain.rootListed()) {
      RootCaReport.Certificate root = chain.chain().get(chain.chain().size() - 1);
      problems.add(broker + " sends root CA " + root.subject() + " sha256=" + chain.rootSha256() + ", which is not in "
                   + RootCaConfig.ROOT_CA_THUMBPRINTS_CONFIG);
    }
  }

  private RootCaReport.BrokerChain brokerChain(Node node) {
    String endpoint = node.host() + ":" + node.port();
    List<X509Certificate> chain;
    try {
      chain = _chainSource.chain(node.host(), node.port());
    } catch (IOException e) {
      return new RootCaReport.BrokerChain(node.id(), endpoint, List.of(), null, null, null, false,
                                          "cannot get its certificate chain: " + e.getMessage());
    }
    String incomplete = CertificateChains.incompleteReason(chain);
    X509Certificate last = chain.get(chain.size() - 1);
    String rootSha256 = CertificateChains.isSelfSigned(last) ? CertificateChains.sha256(last) : null;
    String untrusted = null;
    if (_ccTrust == null) {
      untrusted = "Cruise Control's truststore is not available";
    } else {
      try {
        _ccTrust.checkServerTrusted(chain.toArray(new X509Certificate[0]), chain.get(0).getPublicKey().getAlgorithm());
      } catch (CertificateException | RuntimeException e) {
        untrusted = e.getMessage();
      }
    }
    List<RootCaReport.Certificate> certificates = new ArrayList<>();
    for (int i = 0; i < chain.size(); i++) {
      X509Certificate cert = chain.get(i);
      String role = i == 0 ? "leaf" : (i == chain.size() - 1 && CertificateChains.isSelfSigned(cert)) ? "root" : "intermediate";
      certificates.add(new RootCaReport.Certificate(role, CertificateChains.subject(cert), cert.getIssuerX500Principal().getName(),
                                                    CertificateChains.sha256(cert), Instant.ofEpochMilli(cert.getNotAfter().getTime())
                                                                                           .toString()));
    }
    return new RootCaReport.BrokerChain(node.id(), endpoint, certificates, incomplete, untrusted, rootSha256,
                                        rootSha256 != null && _thumbprints.contains(rootSha256), null);
  }
}
