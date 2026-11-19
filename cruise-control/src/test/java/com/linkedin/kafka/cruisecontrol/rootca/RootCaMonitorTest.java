/*
 * Copyright 2026 qjwu. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.rootca;

import java.io.IOException;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.utils.MockTime;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class RootCaMonitorTest {
  private static TestCertificates.Issued root1;
  private static TestCertificates.Issued root2;
  private static List<X509Certificate> chain1;
  private static List<X509Certificate> chain2;
  private static String sha1;
  private static String sha2;

  private static final List<Node> TWO_BROKERS = Arrays.asList(new Node(1, "localhost", 9092), new Node(2, "localhost", 9094));

  /**
   * Two PKIs: root 1 -> intermediate 1 -> broker, root 2 -> intermediate 2 -> broker.
   *
   * @throws Exception On error.
   */
  @BeforeClass
  public static void setUpPki() throws Exception {
    root1 = TestCertificates.root("CN=Local Kafka CA,O=Test");
    TestCertificates.Issued int1 = TestCertificates.issue("CN=Kafka Intermediate CA,O=Test", root1, true);
    chain1 = Arrays.asList(TestCertificates.issue("CN=broker,O=Test", int1, false).cert(), int1.cert(), root1.cert());
    root2 = TestCertificates.root("CN=Local Kafka CA 2,O=Test");
    TestCertificates.Issued int2 = TestCertificates.issue("CN=Kafka Intermediate CA 2,O=Test", root2, true);
    chain2 = Arrays.asList(TestCertificates.issue("CN=broker,O=Test", int2, false).cert(), int2.cert(), root2.cert());
    sha1 = CertificateChains.sha256(root1.cert());
    sha2 = CertificateChains.sha256(root2.cert());
  }

  private static X509TrustManager trusting(X509Certificate... cas) throws Exception {
    KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
    ks.load(null, null);
    for (int i = 0; i < cas.length; i++) {
      ks.setCertificateEntry("ca" + i, cas[i]);
    }
    TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    tmf.init(ks);
    for (TrustManager tm : tmf.getTrustManagers()) {
      if (tm instanceof X509TrustManager) {
        return (X509TrustManager) tm;
      }
    }
    throw new IllegalStateException();
  }

  /**
   * @param thumbprints Configured thumbprints.
   * @param b1 Chain sent by the broker on port 9092.
   * @param b2 Chain sent by the broker on port 9094.
   * @return A monitor whose brokers send the given chains.
   * @throws Exception On error.
   */
  private static RootCaMonitor monitor(List<String> thumbprints, List<X509Certificate> b1, List<X509Certificate> b2)
      throws Exception {
    Map<Integer, List<X509Certificate>> chains = new HashMap<>();
    chains.put(9092, b1);
    chains.put(9094, b2);
    return new RootCaMonitor(thumbprints, () -> TWO_BROKERS, (host, port) -> chains.get(port), trusting(root1.cert()),
                             "SSL", 60_000L, new MockTime());
  }

  /**
   * Every broker sends a complete chain ending in a listed root: consistent.
   *
   * @throws Exception On error.
   */
  @Test
  public void consistentWhenEveryBrokerSendsACompleteChainWithAListedRoot() throws Exception {
    RootCaReport report = monitor(List.of(sha1), chain1, chain1).report();
    assertTrue(report.problems().toString(), report.consistent());
    assertEquals(1, report.thumbprints().size());
    assertEquals("IN_USE", report.thumbprints().get(0).status());
    assertEquals(List.of(1, 2), report.thumbprints().get(0).brokerIds());
    assertEquals(CertificateChains.subject(root1.cert()), report.thumbprints().get(0).certificate().subject());
    RootCaReport.BrokerChain b1 = report.brokers().get(0);
    assertTrue(b1.complete());
    assertTrue(b1.rootListed());
    assertTrue(b1.trustedByCruiseControl());
    assertEquals(Arrays.asList("leaf", "intermediate", "root"), Arrays.asList(
        b1.chain().get(0).role(), b1.chain().get(1).role(), b1.chain().get(2).role()));
  }

  /**
   * Root CA rotation: new root announced, brokers switched one by one, old root no longer in use.
   *
   * @throws Exception On error.
   */
  @Test
  public void rootCaRotationSteps() throws Exception {
    // 1. New root announced before any broker sends it: listed, NOT_IN_USE; still consistent.
    RootCaReport announced = monitor(List.of(sha1, sha2), chain1, chain1).report();
    assertTrue(announced.consistent());
    assertEquals(List.of(sha1, sha2), Arrays.asList(announced.thumbprints().get(0).sha256(), announced.thumbprints().get(1).sha256()));
    assertEquals("NOT_IN_USE", announced.thumbprints().get(1).status());
    assertNull(announced.thumbprints().get(1).certificate());
    // 2. Rolling switch: broker 2 already on the new root -> both in use, consistent.
    RootCaReport mixed = monitor(List.of(sha1, sha2), chain1, chain2).report();
    assertTrue(mixed.problems().toString(), mixed.consistent());
    assertEquals(List.of(1), mixed.thumbprints().get(0).brokerIds());
    assertEquals(List.of(2), mixed.thumbprints().get(1).brokerIds());
    // 3. All brokers on the new root: the old one is no longer in use (safe to plan its removal).
    RootCaReport switched = monitor(List.of(sha1, sha2), chain2, chain2).report();
    assertTrue(switched.consistent());
    assertEquals("NOT_IN_USE", switched.thumbprints().get(0).status());
    assertEquals("IN_USE", switched.thumbprints().get(1).status());
  }

  /**
   * A broker sending a root that is not listed makes the state inconsistent.
   *
   * @throws Exception On error.
   */
  @Test
  public void brokerWithAnUnlistedRootIsAProblem() throws Exception {
    RootCaReport report = monitor(List.of(sha1), chain1, chain2).report();
    assertFalse(report.consistent());
    assertFalse(report.brokers().get(1).rootListed());
    assertEquals(sha2, report.brokers().get(1).rootSha256());
    assertFalse(report.brokers().get(1).trustedByCruiseControl());
    assertEquals(1, report.problems().size());
    assertTrue(report.problems().get(0), report.problems().get(0).contains("broker 2") && report.problems().get(0).contains(sha2));
  }

  /**
   * Leaf-only and root-less chains are incomplete.
   *
   * @throws Exception On error.
   */
  @Test
  public void incompleteChainsAreProblems() throws Exception {
    // Leaf only, and leaf + intermediate without the root.
    RootCaReport report = monitor(List.of(sha1), chain1.subList(0, 1), chain1.subList(0, 2)).report();
    assertFalse(report.consistent());
    assertEquals(2, report.problems().size());
    assertFalse(report.brokers().get(0).complete());
    assertNull(report.brokers().get(0).rootSha256());
    assertTrue(report.brokers().get(1).incompleteReason(), report.brokers().get(1).incompleteReason().contains("not a self-signed root"));
    assertEquals("NOT_IN_USE", report.thumbprints().get(0).status());
  }

  /**
   * An intermediate with the real name and issuer but another signing key breaks the chain.
   *
   * @throws Exception On error.
   */
  @Test
  public void forgedIntermediateUnderTheRealRootIsIncomplete() throws Exception {
    // Same name as the real intermediate, "issued by" the real root, but signed by another key.
    TestCertificates.Issued forged = TestCertificates.forge(chain1.get(1), root1.cert(), TestCertificates.keys(), true);
    TestCertificates.Issued leaf = TestCertificates.issue("CN=broker,O=Test", forged, false);
    RootCaReport report = monitor(List.of(sha1), chain1, Arrays.asList(leaf.cert(), forged.cert(), root1.cert())).report();
    assertFalse(report.consistent());
    assertTrue(report.brokers().get(1).incompleteReason(), report.brokers().get(1).incompleteReason().contains("does not verify"));
    assertFalse(report.brokers().get(1).trustedByCruiseControl());
  }

  /**
   * Probe errors, no configured thumbprints and an unavailable broker list are problems.
   *
   * @throws Exception On error.
   */
  @Test
  public void probeErrorsAndMissingConfigAreProblems() throws Exception {
    RootCaMonitor failing = new RootCaMonitor(List.of(sha1), () -> TWO_BROKERS, (host, port) -> {
      throw new IOException("connection refused");
    }, trusting(root1.cert()), "SSL", 60_000L, new MockTime());
    RootCaReport report = failing.report();
    assertFalse(report.consistent());
    assertEquals(2, report.problems().size());
    assertTrue(report.brokers().get(0).error().contains("connection refused"));
    assertFalse(report.brokers().get(0).complete());

    RootCaReport unconfigured = monitor(List.of(), chain1, chain1).report();
    assertFalse(unconfigured.consistent());
    assertTrue(unconfigured.problems().get(0), unconfigured.problems().get(0).contains("root.ca.thumbprints is not set"));

    Supplier<Collection<Node>> noBrokers = () -> {
      throw new IllegalStateException("Cannot list the brokers: timeout");
    };
    RootCaReport noList = new RootCaMonitor(List.of(sha1), noBrokers, (h, p) -> chain1, null, "SSL", 0, new MockTime()).report();
    assertFalse(noList.consistent());
    assertTrue(noList.problems().get(0).contains("Cannot list the brokers"));
  }

  /**
   * Without TLS to the brokers there are no chains to observe.
   *
   * @throws Exception On error.
   */
  @Test
  public void plaintextHasNoChainsToObserve() throws Exception {
    RootCaReport report = new RootCaMonitor(List.of(sha1), () -> TWO_BROKERS, (h, p) -> chain1, null, "PLAINTEXT", 0,
                                            new MockTime()).report();
    assertFalse(report.consistent());
    assertTrue(report.brokers().isEmpty());
    assertTrue(report.problems().get(0).contains("security.protocol=PLAINTEXT"));
  }

  /**
   * Reports are cached for the configured time.
   *
   * @throws Exception On error.
   */
  @Test
  public void reportIsCached() throws Exception {
    AtomicInteger probes = new AtomicInteger();
    MockTime time = new MockTime();
    RootCaMonitor monitor = new RootCaMonitor(List.of(sha1), () -> TWO_BROKERS, (h, p) -> {
      probes.incrementAndGet();
      return chain1;
    }, trusting(root1.cert()), "SSL", 60_000L, time);
    RootCaReport first = monitor.report();
    assertSame(first, monitor.report());
    assertEquals(2, probes.get());
    time.sleep(60_000L);
    monitor.report();
    assertEquals(4, probes.get());
  }

  /**
   * Thumbprints: colons, case and duplicates normalized; invalid ones rejected.
   */
  @Test
  public void thumbprintsAreNormalized() {
    String upperWithColons = sha1.toUpperCase().replaceAll("(..)(?!$)", "$1:");
    assertEquals(List.of(sha1, sha2), CertificateChains.normalizeThumbprints(Arrays.asList(upperWithColons, sha2, sha1, " ")));
    assertThrows(IllegalArgumentException.class, () -> CertificateChains.normalizeThumbprints(List.of("1234")));
    assertThrows(IllegalArgumentException.class, () -> CertificateChains.normalizeThumbprints(List.of(sha1.substring(1) + "z")));
  }
}
