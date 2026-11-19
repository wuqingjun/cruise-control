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
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
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
