/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.acl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.ManualMetadataUpdater;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.NetworkClientUtils;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.requests.DescribeAclsRequest;
import org.apache.kafka.common.requests.DescribeAclsResponse;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Verifies <b>every live broker</b>: the broker list comes from {@code describeCluster}; each broker gets its own
 * {@code DescribeAcls} request (answered from that broker's ACL cache), compared with ZooKeeper's tree. Brokers that
 * don't match yet are asked again until all match or the timeout passes; the result names each broker that still
 * differs or couldn't be reached.
 * <p>
 * The admin API can't send a request to a chosen broker, so this class uses Kafka's client-internal classes
 * ({@link NetworkClient}, {@link DescribeAclsRequest}); like {@link KafkaAclZkCodec}, it is Kafka-version-specific and
 * the only place that uses them (built against kafka-clients 3.5.2; the request version is negotiated with the broker).
 * Connections use Cruise Control's admin client settings (security protocol, SSL keystore / truststore, SASL).
 */
public class PerBrokerAclSyncVerifier implements AclSyncVerifier {
  private static final Logger LOG = LoggerFactory.getLogger(PerBrokerAclSyncVerifier.class);
  private static final String CLIENT_ID = "cruise-control-acl-sync-verifier";
  private static final long RETRY_BACKOFF_MS = 500;
  private static final long MIN_ATTEMPT_MS = 1_000;

  /** Supplies the brokers to check. */
  @FunctionalInterface
  interface BrokerSupplier {
    /**
     * @param timeoutMs Time limit.
     * @return Live brokers.
     * @throws Exception If they can't be listed.
     */
    Collection<Node> brokers(long timeoutMs) throws Exception;
  }

  private final BrokerSupplier _brokers;
  private final Map<String, Object> _clientConfigs;
  private final Time _time;
  private Metrics _metrics;
  private NetworkClient _client;

  /**
   * @param admin Admin client (for {@code describeCluster}).
   * @param clientConfigs Cruise Control's admin client configs (bootstrap servers, security settings).
   * @param time Time.
   */
  public PerBrokerAclSyncVerifier(Admin admin, Map<String, Object> clientConfigs, Time time) {
    this(timeoutMs -> admin.describeCluster(new DescribeClusterOptions().timeoutMs((int) timeoutMs)).nodes()
                           .get(timeoutMs, TimeUnit.MILLISECONDS), clientConfigs, time);
  }

  /**
   * @param brokers Supplies the brokers to check.
   * @param clientConfigs Client configs.
   * @param time Time.
   */
  PerBrokerAclSyncVerifier(BrokerSupplier brokers, Map<String, Object> clientConfigs, Time time) {
    _brokers = brokers;
    _clientConfigs = new HashMap<>(clientConfigs);
    _time = time;
  }

  @Override
  public synchronized AclVerifyResult verify(AclTree expected, long timeoutMs) throws InterruptedException {
    long deadline = _time.milliseconds() + timeoutMs;
    Collection<Node> nodes;
    try {
      nodes = _brokers.brokers(Math.max(1, timeoutMs));
    } catch (InterruptedException e) {
      throw e;
    } catch (Exception e) {
      throw new IllegalStateException("Couldn't list the brokers (describeCluster) to verify their ACLs: " + e, e);
    }
    Map<Integer, Node> pending = new LinkedHashMap<>();
    nodes.forEach(n -> pending.put(n.id(), n));
    Map<Integer, List<ResourcePattern>> differences = new HashMap<>();
    Map<Integer, String> unreachable = new HashMap<>();
    NetworkClient client = client();
    while (true) {
      for (Node node : new ArrayList<>(pending.values())) {
        // a broker that doesn't answer must not use up the time of the others
        long attemptMs = Math.max(MIN_ATTEMPT_MS, Math.min(deadline - _time.milliseconds(), timeoutMs / 3));
        try {
          List<ResourcePattern> different = AdminAclSyncVerifier.differences(describe(client, node, attemptMs), expected);
          unreachable.remove(node.id());
          if (different.isEmpty()) {
            pending.remove(node.id());
            differences.remove(node.id());
          } else {
            differences.put(node.id(), different);
          }
        } catch (IOException | RuntimeException e) {
          LOG.debug("Broker {} not checked: {}", node.id(), e.toString());
          unreachable.put(node.id(), String.valueOf(e.getMessage()));
          differences.remove(node.id());
        }
      }
      if (pending.isEmpty() || _time.milliseconds() + RETRY_BACKOFF_MS > deadline) {
        break;
      }
      _time.sleep(RETRY_BACKOFF_MS);
    }
    if (pending.isEmpty()) {
      LOG.debug("All {} broker(s) report the expected ACLs.", nodes.size());
      return AclVerifyResult.ok();
    }
    differences.keySet().retainAll(pending.keySet());
    unreachable.keySet().retainAll(pending.keySet());
    return AclVerifyResult.of(differences, unreachable);
  }

  /**
   * Ask one broker for all its ACLs.
   *
   * @param client Network client.
   * @param node The broker.
   * @param timeoutMs Time limit for connecting.
   * @return What the broker's ACL cache holds.
   * @throws IOException If the broker can't be reached or returns an error.
   */
  private AclTree describe(NetworkClient client, Node node, long timeoutMs) throws IOException {
    if (!NetworkClientUtils.awaitReady(client, node, _time, timeoutMs)) {
      throw new IOException("not ready within " + timeoutMs + " ms");
    }
    ClientRequest request = client.newClientRequest(node.idString(), new DescribeAclsRequest.Builder(AclBindingFilter.ANY),
                                                    _time.milliseconds(), true);
    ClientResponse response = NetworkClientUtils.sendAndReceive(client, request, _time);
    if (response.versionMismatch() != null) {
      throw new IOException("DescribeAcls not supported: " + response.versionMismatch().getMessage());
    }
    if (response.authenticationException() != null) {
      throw new IOException("authentication failed: " + response.authenticationException().getMessage());
    }
    DescribeAclsResponse body = (DescribeAclsResponse) response.responseBody();
    if (body.error().isFailure()) {
      throw new IOException("DescribeAcls failed: " + body.error());
    }
    return AdminAclSyncVerifier.toTree(DescribeAclsResponse.aclBindings(body.acls()));
  }

  /**
   * @return The network client, created on first use with the admin client's settings.
   */
  private NetworkClient client() {
    if (_client == null) {
      AdminClientConfig config = new AdminClientConfig(_clientConfigs);
      LogContext logContext = new LogContext("[ACL sync verifier] ");
      ChannelBuilder channelBuilder = ClientUtils.createChannelBuilder(config, _time, logContext);
      _metrics = new Metrics(_time);
      Selector selector = new Selector(config.getLong(AdminClientConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG), _metrics, _time,
                                       "acl-sync-verifier", channelBuilder, logContext);
      _client = new NetworkClient(selector, new ManualMetadataUpdater(), CLIENT_ID, 1,
                                  config.getLong(AdminClientConfig.RECONNECT_BACKOFF_MS_CONFIG),
                                  config.getLong(AdminClientConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG),
                                  config.getInt(AdminClientConfig.SEND_BUFFER_CONFIG),
                                  config.getInt(AdminClientConfig.RECEIVE_BUFFER_CONFIG),
                                  config.getInt(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG),
                                  config.getLong(AdminClientConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG),
                                  config.getLong(AdminClientConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG),
                                  _time, true, new ApiVersions(), logContext);
    }
    return _client;
  }

  @Override
  public synchronized void close() {
    if (_client != null) {
      try {
        _client.close();
      } catch (RuntimeException e) {
        LOG.debug("Closing the verifier's network client failed.", e);
      }
      _client = null;
    }
    if (_metrics != null) {
      _metrics.close();
      _metrics = null;
    }
  }
}
