/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.acl;

import com.linkedin.kafka.cruisecontrol.acl.AclTreeDiff.ResourceChange;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.kafka.common.resource.ResourcePattern;

/**
 * Drift found while the file was unchanged (G19): ZooKeeper differs from the approved file, and / or the brokers serve
 * ACLs that differ from ZooKeeper (e.g. a data node edited without a change notification).
 */
public final class AclDrift {
  private final String _sha256;
  private final List<ResourceChange> _zkChanges;
  private final AclVerifyResult _brokers;
  private final boolean _reapplied;

  /**
   * @param sha256 Hash of the approved file.
   * @param zkChanges What it takes to bring ZooKeeper back to the file (current = ZooKeeper, desired = file).
   * @param brokers Per broker, the resources it serves differently from ZooKeeper, and brokers that couldn't be asked.
   * @param reapplied Whether Cruise Control repaired it ({@code acl.sync.drift.action=REAPPLY}).
   */
  public AclDrift(String sha256, List<ResourceChange> zkChanges, AclVerifyResult brokers, boolean reapplied) {
    _sha256 = sha256;
    _zkChanges = Collections.unmodifiableList(new ArrayList<>(zkChanges));
    _brokers = brokers;
    _reapplied = reapplied;
  }

  /**
   * @param sha256 Hash of the approved file.
   * @param zkChanges What it takes to bring ZooKeeper back to the file.
   * @param brokerDifferences Resources a broker (not identified) reports differently from ZooKeeper.
   * @param reapplied Whether Cruise Control repaired it.
   */
  public AclDrift(String sha256, List<ResourceChange> zkChanges, List<ResourcePattern> brokerDifferences, boolean reapplied) {
    this(sha256, zkChanges, AclVerifyResult.anyBroker(brokerDifferences), reapplied);
  }

  /**
   * @return Hash of the approved file.
   */
  public String sha256() {
    return _sha256;
  }

  /**
   * @return Differences between ZooKeeper and the file.
   */
  public List<ResourceChange> zkChanges() {
    return _zkChanges;
  }

  /**
   * @return Resources the brokers report differently from ZooKeeper.
   */
  public List<ResourcePattern> brokerDifferences() {
    return _brokers.resources();
  }

  /**
   * @return Per broker, what it serves differently from ZooKeeper, and brokers that couldn't be asked.
   */
  public AclVerifyResult brokers() {
    return _brokers;
  }

  /**
   * @return Whether the drift was repaired.
   */
  public boolean reapplied() {
    return _reapplied;
  }

  @Override
  public String toString() {
    List<String> zk = new ArrayList<>();
    _zkChanges.forEach(c -> zk.add(c.type() + " " + AclTreeParser.location(c.pattern())));
    return String.format("ACL drift (file sha256 %s, %s): ZooKeeper vs file %s; brokers vs ZooKeeper: %s", _sha256,
                         _reapplied ? "re-applied" : "reported only", zk, _brokers.describe());
  }
}
