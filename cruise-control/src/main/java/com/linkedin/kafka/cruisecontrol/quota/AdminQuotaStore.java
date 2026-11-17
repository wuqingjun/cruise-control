/*
 * Copyright 2026 qjwu. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.quota;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterClientQuotasOptions;
import org.apache.kafka.clients.admin.DescribeClientQuotasOptions;
import org.apache.kafka.common.quota.ClientQuotaAlteration;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.common.quota.ClientQuotaFilter;

/**
 * Kafka's quota storage through the admin API (decision C1): {@code describeClientQuotas} / {@code alterClientQuotas},
 * entity type {@code user}, the user name being the identity (e.g. {@code Claim:payments-team}) and the default user
 * entity for {@link QuotaSet#DEFAULT}. A broker validates and stores them (ZooKeeper {@code /config/users}) and all
 * brokers pick them up.
 */
public class AdminQuotaStore implements QuotaStore {
  private final Admin _admin;
  private final long _timeoutMs;

  /**
   * @param admin Admin client (its principal must be allowed to change quotas).
   * @param timeoutMs Timeout of each request.
   */
  public AdminQuotaStore(Admin admin, long timeoutMs) {
    _admin = admin;
    _timeoutMs = timeoutMs;
  }

  @Override
  public Stored read() throws Exception {
    Map<ClientQuotaEntity, Map<String, Double>> entries = _admin.describeClientQuotas(
        ClientQuotaFilter.all(), new DescribeClientQuotasOptions().timeoutMs((int) _timeoutMs))
        .entities().get(_timeoutMs, TimeUnit.MILLISECONDS);
    Map<String, Map<String, Double>> userQuotas = new HashMap<>();
    List<String> ignored = new ArrayList<>();
    entries.forEach((entity, values) -> {
      Map<String, String> parts = entity.entries();
      if (parts.size() == 1 && parts.containsKey(ClientQuotaEntity.USER)) {
        String user = parts.get(ClientQuotaEntity.USER);
        userQuotas.put(user == null ? QuotaSet.DEFAULT : user, managedValues(values, entity, ignored));
      } else {
        ignored.add(entity + " " + values);
      }
    });
    Collections.sort(ignored);
    return new Stored(new QuotaSet(userQuotas), ignored);
  }

  /**
   * Values quota sync manages; other quota keys of a user entity (e.g. controller_mutation_rate) are left alone.
   *
   * @param values Stored values of one user entity.
   * @param entity The entity (for logging).
   * @param ignored Collects the values left alone.
   * @return The managed values.
   */
  private static Map<String, Double> managedValues(Map<String, Double> values, ClientQuotaEntity entity, List<String> ignored) {
    Map<String, Double> managed = new HashMap<>();
    values.forEach((key, value) -> {
      if (QuotaSet.QUOTA_KEYS.contains(key)) {
        managed.put(key, value);
      } else {
        ignored.add(entity + " " + key + "=" + value);
      }
    });
    return managed;
  }

  @Override
  public void apply(List<QuotaDiff.Change> changes) throws Exception {
    if (changes.isEmpty()) {
      return;
    }
    List<ClientQuotaAlteration> alterations = new ArrayList<>();
    for (QuotaDiff.Change change : changes) {
      List<ClientQuotaAlteration.Op> ops = new ArrayList<>();
      change.set().forEach((key, value) -> ops.add(new ClientQuotaAlteration.Op(key, value)));
      change.remove().forEach(key -> ops.add(new ClientQuotaAlteration.Op(key, null)));
      alterations.add(new ClientQuotaAlteration(entity(change.identity()), ops));
    }
    _admin.alterClientQuotas(alterations, new AlterClientQuotasOptions().timeoutMs((int) _timeoutMs))
          .all().get(_timeoutMs, TimeUnit.MILLISECONDS);
  }

  /**
   * @param identity Identity or {@link QuotaSet#DEFAULT}.
   * @return The {@code user} quota entity (default user entity for the default).
   */
  static ClientQuotaEntity entity(String identity) {
    return new ClientQuotaEntity(Collections.singletonMap(ClientQuotaEntity.USER, QuotaSet.DEFAULT.equals(identity) ? null : identity));
  }
}
