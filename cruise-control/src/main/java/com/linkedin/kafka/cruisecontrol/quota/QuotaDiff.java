/*
 * Copyright 2026 qjwu. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.quota;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * What it takes to turn the stored quotas into the desired ones: per identity, the values to set and the quota keys to
 * remove. The desired set owns every identity (decision C3): identities only in the stored set lose all their values.
 */
public final class QuotaDiff {

  /** One identity's change. */
  public static final class Change {
    /** Added = only desired, removed = only stored, updated = both. */
    public enum Type { ADDED, UPDATED, REMOVED }

    private final String _identity;
    private final Type _type;
    private final SortedMap<String, Double> _set;
    private final SortedSet<String> _remove;

    Change(String identity, Type type, SortedMap<String, Double> set, SortedSet<String> remove) {
      _identity = identity;
      _type = type;
      _set = Collections.unmodifiableSortedMap(set);
      _remove = Collections.unmodifiableSortedSet(remove);
    }

    /**
     * @return Identity, or {@link QuotaSet#DEFAULT}.
     */
    public String identity() {
      return _identity;
    }

    /**
     * @return Added, updated or removed.
     */
    public Type type() {
      return _type;
    }

    /**
     * @return Quota key → new value.
     */
    public SortedMap<String, Double> set() {
      return _set;
    }

    /**
     * @return Quota keys to remove.
     */
    public SortedSet<String> remove() {
      return _remove;
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof Change)) {
        return false;
      }
      Change other = (Change) o;
      return _identity.equals(other._identity) && _type == other._type && _set.equals(other._set) && _remove.equals(other._remove);
    }

    @Override
    public int hashCode() {
      return Objects.hash(_identity, _type, _set, _remove);
    }

    @Override
    public String toString() {
      return String.format("%s %s: set %s, remove %s", _type, _identity, _set, _remove);
    }
  }

  private QuotaDiff() {
  }

  /**
   * @param stored What Kafka has (user quotas).
   * @param desired The file.
   * @return Changes in identity order; empty if equal.
   */
  public static List<Change> diff(QuotaSet stored, QuotaSet desired) {
    SortedSet<String> identities = new TreeSet<>(stored.quotas().keySet());
    identities.addAll(desired.quotas().keySet());
    List<Change> changes = new ArrayList<>();
    for (String identity : identities) {
      Map<String, Double> before = stored.get(identity);
      Map<String, Double> after = desired.get(identity);
      SortedMap<String, Double> set = new TreeMap<>();
      after.forEach((key, value) -> {
        if (!value.equals(before.get(key))) {
          set.put(key, value);
        }
      });
      SortedSet<String> remove = new TreeSet<>(before.keySet());
      remove.removeAll(after.keySet());
      if (set.isEmpty() && remove.isEmpty()) {
        continue;
      }
      Change.Type type = before.isEmpty() ? Change.Type.ADDED : after.isEmpty() ? Change.Type.REMOVED : Change.Type.UPDATED;
      changes.add(new Change(identity, type, set, remove));
    }
    return changes;
  }
}
