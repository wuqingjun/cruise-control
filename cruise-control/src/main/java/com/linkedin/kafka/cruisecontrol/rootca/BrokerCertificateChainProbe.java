/*
 * Copyright 2026 qjwu. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.rootca;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.security.GeneralSecurityException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import javax.net.ssl.SNIHostName;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509ExtendedTrustManager;

/**
 * Records the certificate chain a broker sends during the TLS handshake. The handshake is aborted right after the
 * broker's certificates are received: no client certificate is needed and no Kafka request is sent. The recorded chain
 * is not trusted by this class; it is only reported (and validated separately).
 */
public class BrokerCertificateChainProbe {
  private static final Pattern IP_ADDRESS = Pattern.compile("^[0-9.]+$|^[0-9a-fA-F:]+$");

  /** Thrown by the recording trust manager to stop the handshake once the chain is recorded. */
  private static final class ChainRecorded extends CertificateException {
    private static final long serialVersionUID = 1L;

    ChainRecorded() {
      super("certificate chain recorded by the probe; handshake intentionally aborted");
    }
  }

  /** Accepts nothing: records the server's chain, then aborts. */
  private static final class RecordingTrustManager extends X509ExtendedTrustManager {
    private final AtomicReference<X509Certificate[]> _chain;

    RecordingTrustManager(AtomicReference<X509Certificate[]> chain) {
      _chain = chain;
    }

    private void record(X509Certificate[] chain) throws CertificateException {
      _chain.set(chain == null ? new X509Certificate[0] : chain.clone());
      throw new ChainRecorded();
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType, Socket socket) throws CertificateException {
      record(chain);
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType, SSLEngine engine) throws CertificateException {
      record(chain);
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
      record(chain);
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType, Socket socket) throws CertificateException {
      throw new CertificateException("not a server");
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType, SSLEngine engine) throws CertificateException {
      throw new CertificateException("not a server");
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
      throw new CertificateException("not a server");
    }

    @Override
    public X509Certificate[] getAcceptedIssuers() {
      return new X509Certificate[0];
    }
  }

  private final int _timeoutMs;

  /**
   * @param timeoutMs Connect and handshake timeout.
   */
  public BrokerCertificateChainProbe(int timeoutMs) {
    _timeoutMs = timeoutMs;
  }

  /**
   * @param host Broker host.
   * @param port Broker port (a TLS listener).
   * @return The certificate chain the broker sent, leaf first.
   * @throws IOException If the broker can't be reached or didn't send a certificate chain (e.g. not a TLS listener).
   */
  public List<X509Certificate> probe(String host, int port) throws IOException {
    AtomicReference<X509Certificate[]> recorded = new AtomicReference<>();
    SSLContext context;
    try {
      context = SSLContext.getInstance("TLS");
      context.init(null, new TrustManager[]{new RecordingTrustManager(recorded)}, null);
    } catch (GeneralSecurityException e) {
      throw new IOException("Cannot create an SSL context: " + e.getMessage(), e);
    }
    try (Socket plain = new Socket()) {
      plain.connect(new InetSocketAddress(host, port), _timeoutMs);
      plain.setSoTimeout(_timeoutMs);
      try (SSLSocket socket = (SSLSocket) context.getSocketFactory().createSocket(plain, host, port, false)) {
        if (!IP_ADDRESS.matcher(host).matches()) {
          SSLParameters parameters = socket.getSSLParameters();
          parameters.setServerNames(Collections.singletonList(new SNIHostName(host)));
          socket.setSSLParameters(parameters);
        }
        try {
          socket.startHandshake();
        } catch (IOException e) {
          if (recorded.get() == null) {
            throw new IOException("No certificate chain received from " + host + ":" + port + " (is it a TLS listener?): "
                                  + e.getMessage(), e);
          }
        }
      }
    }
    X509Certificate[] chain = recorded.get();
    if (chain == null || chain.length == 0) {
      throw new IOException("No certificate chain received from " + host + ":" + port);
    }
    return Arrays.asList(chain);
  }
}
