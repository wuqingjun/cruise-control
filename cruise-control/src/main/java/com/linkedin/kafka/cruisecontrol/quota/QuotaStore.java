/*
 * Copyright 2026 qjwu. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.quota;

import java.util.List;

/**
 * Where the quotas are stored (Kafka's quota storage, through the admin API — decision C1).
 */
public interface QuotaStore {

  /** Stored quotas: the user quotas quota sync manages, and the other entries it leaves alone. */
  final class Stored {
    private final QuotaSet _userQuotas;
    private final List<String> _ignored;

    /**
     * @param userQuotas Quotas of entity type {@code user} only (name = identity; default user = {@link QuotaSet#DEFAULT}).
     * @param ignored Other entries ({@code client-id}, {@code (user, client-id)}, …), described for logging.
     */
    public Stored(QuotaSet userQuotas, List<String> ignored) {
      _userQuotas = userQuotas;
      _ignored = List.copyOf(ignored);
    }

    /**
     * @return The managed user quotas.
     */
    public QuotaSet userQuotas() {
      return _userQuotas;
    }

    /**
     * @return Entries left alone (decision C3: they have no effect with MultiIdentityQuotaCallback).
     */
    public List<String> ignored() {
      return _ignored;
    }
  }

  /**
   * @return What is stored.
   * @throws Exception If the quotas can't be read.
   */
  Stored read() throws Exception;

  /**
   * Apply changes; each identity is changed on its own (not atomic across identities).
   *
   * @param changes From {@link QuotaDiff#diff}.
   * @throws Exception If the quotas can't be changed (e.g. refused by the brokers).
   */
  void apply(List<QuotaDiff.Change> changes) throws Exception;
}
