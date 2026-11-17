/*
 * Copyright 2026 qjwu. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.quota;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Client quotas by identity: identity ({@code Type:name}, or {@link #DEFAULT} for principals without a matching
 * identity) → quota key ({@link #QUOTA_KEYS}) → value. Used both for the file and for what Kafka has stored (quotas of
 * entity type {@code user}, whose name is the identity). Sorted, so comparisons and logs are stable.
 */
public final class QuotaSet {
  /** Key of the default entry (Kafka's default user entity). Can't clash with an identity: identities contain ':'. */
  public static final String DEFAULT = "<default>";
  public static final String PRODUCER_BYTE_RATE = "producer_byte_rate";
  public static final String CONSUMER_BYTE_RATE = "consumer_byte_rate";
  public static final String REQUEST_PERCENTAGE = "request_percentage";
  /** Quota keys managed by quota sync (v1). */
  public static final List<String> QUOTA_KEYS = List.of(PRODUCER_BYTE_RATE, CONSUMER_BYTE_RATE, REQUEST_PERCENTAGE);

  private final SortedMap<String, SortedMap<String, Double>> _quotas;

  /**
   * @param quotas Identity → quota key → value; identities without values are dropped.
   */
  public QuotaSet(Map<String, ? extends Map<String, Double>> quotas) {
    SortedMap<String, SortedMap<String, Double>> copy = new TreeMap<>();
    quotas.forEach((identity, values) -> {
      if (!values.isEmpty()) {
        copy.put(identity, Collections.unmodifiableSortedMap(new TreeMap<>(values)));
      }
    });
    _quotas = Collections.unmodifiableSortedMap(copy);
  }

  /**
   * @return Identity → quota key → value (sorted).
   */
  public SortedMap<String, SortedMap<String, Double>> quotas() {
    return _quotas;
  }

  /**
   * @param identity Identity or {@link #DEFAULT}.
   * @return Its values (empty if none).
   */
  public SortedMap<String, Double> get(String identity) {
    SortedMap<String, Double> values = _quotas.get(identity);
    return values == null ? Collections.emptySortedMap() : values;
  }

  /**
   * @return Number of identities (including the default) with values.
   */
  public int size() {
    return _quotas.size();
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof QuotaSet && _quotas.equals(((QuotaSet) o)._quotas);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_quotas);
  }

  @Override
  public String toString() {
    return _quotas.toString();
  }
}
