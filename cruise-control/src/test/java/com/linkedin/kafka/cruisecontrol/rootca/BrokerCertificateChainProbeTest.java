/*
 * Copyright 2026 qjwu. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.rootca;

import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManagerFactory;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class BrokerCertificateChainProbeTest {

  /**
   * Accepts connections in the background until closed.
   *
   * @param server The server socket.
   * @param tls Whether to do a TLS handshake or to answer in plain text.
   * @return The thread.
   */
  private static Thread serve(ServerSocket server, boolean tls) {
    Thread t = new Thread(() -> {
      while (!server.isClosed()) {
        try (Socket s = server.accept()) {
          if (tls) {
            try {
              ((SSLSocket) s).startHandshake();
            } catch (IOException expected) {
              // the probe aborts the handshake
            }
          } else {
            OutputStream out = s.getOutputStream();
            out.write("not TLS\n".getBytes(StandardCharsets.US_ASCII));
            out.flush();
          }
        } catch (IOException e) {
          return;
        }
      }
    });
    t.setDaemon(true);
    t.start();
    return t;
  }

  /**
   * A TLS server sending leaf -> intermediate -> root, requiring client certificates like a Kafka SSL listener.
   *
   * @throws Exception On error.
   */
  @Test
  public void recordsTheFullChainTheServerSends() throws Exception {
    TestCertificates.Issued root = TestCertificates.root("CN=Probe Root,O=Test");
    TestCertificates.Issued intermediate = TestCertificates.issue("CN=Probe Intermediate,O=Test", root, true);
    TestCertificates.Issued leaf = TestCertificates.issue("CN=localhost,O=Test", intermediate, false);
    X509Certificate[] chain = {leaf.cert(), intermediate.cert(), root.cert()};
    char[] password = "test".toCharArray();
    KeyStore ks = KeyStore.getInstance("PKCS12");
    ks.load(null, null);
    ks.setKeyEntry("server", leaf.keys().getPrivate(), password, chain);
    KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    kmf.init(ks, password);
    SSLContext context = SSLContext.getInstance("TLS");
    context.init(kmf.getKeyManagers(), null, null);
    try (SSLServerSocket server = (SSLServerSocket) context.getServerSocketFactory().createServerSocket(0)) {
      // Like a Kafka SSL listener with ssl.client.auth=required: the chain is still recorded (no client cert needed).
      server.setNeedClientAuth(true);
      serve(server, true);
      List<X509Certificate> recorded = new BrokerCertificateChainProbe(5_000).probe("localhost", server.getLocalPort());
      assertArrayEquals(chain, recorded.toArray(new Certificate[0]));
    }
  }

  /**
   * A plain-text listener and a closed port fail with an IOException.
   *
   * @throws Exception On error.
   */
  /**
   * With Cruise Control's client certificate the handshake completes (the broker authenticates the probe instead of
   * logging a failed handshake); without it the handshake is aborted. The chain is recorded either way.
   *
   * @throws Exception On error.
   */
  @Test
  public void completesTheHandshakeWithAClientCertificate() throws Exception {
    TestCertificates.Issued root = TestCertificates.root("CN=Probe Root 2,O=Test");
    TestCertificates.Issued leaf = TestCertificates.issue("CN=localhost,O=Test", root, false);
    TestCertificates.Issued client = TestCertificates.issue("CN=cruise-control,O=Test", root, false);
    char[] password = "test".toCharArray();
    KeyStore serverKeys = KeyStore.getInstance("PKCS12");
    serverKeys.load(null, null);
    serverKeys.setKeyEntry("server", leaf.keys().getPrivate(), password, new Certificate[]{leaf.cert(), root.cert()});
    KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    kmf.init(serverKeys, password);
    KeyStore serverTrust = KeyStore.getInstance("PKCS12");
    serverTrust.load(null, null);
    serverTrust.setCertificateEntry("root", root.cert());
    TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    tmf.init(serverTrust);
    SSLContext serverContext = SSLContext.getInstance("TLS");
    serverContext.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);

    KeyStore clientKeys = KeyStore.getInstance("PKCS12");
    clientKeys.load(null, null);
    clientKeys.setKeyEntry("client", client.keys().getPrivate(), password, new Certificate[]{client.cert(), root.cert()});
    KeyManagerFactory ckmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    ckmf.init(clientKeys, password);

    try (SSLServerSocket server = (SSLServerSocket) serverContext.getServerSocketFactory().createServerSocket(0)) {
      server.setNeedClientAuth(true);
      AtomicInteger succeeded = new AtomicInteger();
      AtomicInteger failed = new AtomicInteger();
      Thread t = new Thread(() -> {
        while (!server.isClosed()) {
          try (Socket s = server.accept()) {
            try {
              ((SSLSocket) s).startHandshake();
              succeeded.incrementAndGet();
            } catch (IOException e) {
              failed.incrementAndGet();
            }
          } catch (IOException e) {
            return;
          }
        }
      });
      t.setDaemon(true);
      t.start();
      List<X509Certificate> withCert = new BrokerCertificateChainProbe(5_000, ckmf.getKeyManagers()).probe("localhost", server.getLocalPort());
      assertEquals(2, withCert.size());
      waitFor(() -> succeeded.get() + failed.get() >= 1);
      assertEquals("handshake completed with the client certificate", 1, succeeded.get());
      List<X509Certificate> aborted = new BrokerCertificateChainProbe(5_000).probe("localhost", server.getLocalPort());
      assertEquals(2, aborted.size());
      waitFor(() -> succeeded.get() + failed.get() >= 2);
      assertEquals("aborted without a client certificate", 1, failed.get());
    }
  }

  private static void waitFor(java.util.function.BooleanSupplier condition) throws InterruptedException {
    long end = System.currentTimeMillis() + 10_000;
    while (!condition.getAsBoolean() && System.currentTimeMillis() < end) {
      Thread.sleep(20);
    }
  }

  /**
   * A plain-text listener and a closed port fail with an IOException.
   *
   * @throws Exception On error.
   */
  @Test
  public void nonTlsListenerAndClosedPortFail() throws Exception {
    try (ServerSocket plain = new ServerSocket(0)) {
      serve(plain, false);
      assertThrows(IOException.class, () -> new BrokerCertificateChainProbe(5_000).probe("localhost", plain.getLocalPort()));
    }
    int closedPort;
    try (ServerSocket s = new ServerSocket(0)) {
      closedPort = s.getLocalPort();
    }
    assertThrows(IOException.class, () -> new BrokerCertificateChainProbe(2_000).probe("localhost", closedPort));
  }
}
