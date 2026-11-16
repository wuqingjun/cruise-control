/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.acl;

import com.linkedin.kafka.cruisecontrol.acl.AclTreeDiff.ChangeType;
import com.linkedin.kafka.cruisecontrol.acl.AclTreeDiff.ResourceChange;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.SortedSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.client.ZKClientConfig;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cruise Control's access to the brokers' ACL storage in ZooKeeper. The {@link ZooKeeper} handle is connected to the ACL
 * root (the authorizer's connect string including the chroot), so all paths here are relative to it.
 * <ul>
 *   <li>Reads the actual ACLs into an {@link AclTree} ({@link #readCurrent()}), remembering each node's version.</li>
 *   <li>Reads / writes the state node {@value #STATE_NODE} and holds the lock {@value #LOCK_NODE}.</li>
 *   <li>Applies a diff with ZooKeeper {@code multi} batches ({@link #apply}): each resource's data-node operation is followed
 *       by its change notification in the same batch; batches are split at {@code maxBatchBytes}; the state node is written
 *       in the <b>last</b> batch, so it only moves when everything is applied.</li>
 * </ul>
 * Data nodes are created / rewritten / deleted with the version read in {@link #readCurrent()}; if anything changed in the
 * meantime the batch fails as a whole and {@link AclZkConflictException} tells the caller to start over.
 */
public class AclZkStore implements AutoCloseable {
  /** Folder of Cruise Control's ACL sync state, under the ACL root. */
  public static final String STATE_FOLDER = "/cruise-control-acl-sync";
  /** State node: last applied file (hash, time, unmanaged resources). */
  public static final String STATE_NODE = STATE_FOLDER + "/current";
  /** Ephemeral lock node: only one Cruise Control instance applies at a time. */
  public static final String LOCK_NODE = STATE_FOLDER + "/lock";
  private static final Logger LOG = LoggerFactory.getLogger(AclZkStore.class);
  /** Rough per-operation overhead in a multi request, on top of path and data. */
  private static final int OP_OVERHEAD_BYTES = 64;

  private final ZooKeeper _zk;
  private final int _maxBatchBytes;
  private final List<ACL> _nodeAcls;

  /** Current ACLs as read from ZooKeeper, with the data nodes' versions. */
  public static final class Snapshot {
    private final AclTree _tree;
    private final Map<ResourcePattern, Integer> _versions;

    Snapshot(AclTree tree, Map<ResourcePattern, Integer> versions) {
      _tree = tree;
      _versions = Collections.unmodifiableMap(versions);
    }

    /**
     * @return The ACLs in ZooKeeper (resources with at least one entry).
     */
    public AclTree tree() {
      return _tree;
    }

    /**
     * @param pattern Resource pattern.
     * @return Version of the resource's data node, if the node exists (also for an existing node without entries).
     */
    public Optional<Integer> version(ResourcePattern pattern) {
      return Optional.ofNullable(_versions.get(pattern));
    }
  }

  /** The state node and its version. */
  public static final class VersionedState {
    private final AclStateNode _state;
    private final int _version;

    VersionedState(AclStateNode state, int version) {
      _state = state;
      _version = version;
    }

    public AclStateNode state() {
      return _state;
    }

    public int version() {
      return _version;
    }
  }

  /** What an apply did. */
  public static final class ApplyResult {
    private final int _batches;
    private final int _operations;
    private final int _notifications;

    ApplyResult(int batches, int operations, int notifications) {
      _batches = batches;
      _operations = operations;
      _notifications = notifications;
    }

    public int batches() {
      return _batches;
    }

    public int operations() {
      return _operations;
    }

    public int notifications() {
      return _notifications;
    }

    @Override
    public String toString() {
      return String.format("ApplyResult(batches=%d, operations=%d, notifications=%d)", _batches, _operations, _notifications);
    }
  }

  /**
   * @param zk ZooKeeper handle connected to the ACL root (chroot included).
   * @param maxBatchBytes Maximum estimated size of one multi request.
   * @param nodeAcls ZooKeeper ACLs for created nodes (open until ZooKeeper security is enabled).
   */
  public AclZkStore(ZooKeeper zk, int maxBatchBytes, List<ACL> nodeAcls) {
    _zk = Objects.requireNonNull(zk, "zk");
    _maxBatchBytes = maxBatchBytes;
    // Not List.copyOf: ZooKeeper's validateACL calls contains(null), which immutable lists reject with an NPE.
    _nodeAcls = Collections.unmodifiableList(new ArrayList<>(nodeAcls));
  }

  /**
   * @param zk ZooKeeper handle connected to the ACL root (chroot included).
   * @param maxBatchBytes Maximum estimated size of one multi request.
   */
  public AclZkStore(ZooKeeper zk, int maxBatchBytes) {
    this(zk, maxBatchBytes, ZooDefs.Ids.OPEN_ACL_UNSAFE);
  }

  /**
   * Connect a ZooKeeper handle and wait until it is connected.
   *
   * @param connectString ACL root connect string, e.g. {@code localhost:2181/kafka}.
   * @param clientConfig ZooKeeper client config (TLS etc.).
   * @param sessionTimeoutMs Session timeout.
   * @param connectTimeoutMs How long to wait for the connection.
   * @return A connected handle.
   * @throws IOException If the connection can't be established in time.
   */
  public static ZooKeeper connect(String connectString, ZKClientConfig clientConfig, int sessionTimeoutMs, long connectTimeoutMs)
      throws IOException {
    CountDownLatch connected = new CountDownLatch(1);
    Watcher watcher = event -> {
      if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
        connected.countDown();
      }
    };
    ZooKeeper zk = clientConfig == null ? new ZooKeeper(connectString, sessionTimeoutMs, watcher)
                                        : new ZooKeeper(connectString, sessionTimeoutMs, watcher, clientConfig);
    try {
      if (!connected.await(connectTimeoutMs, TimeUnit.MILLISECONDS)) {
        closeQuietly(zk);
        throw new IOException("Timed out connecting to ZooKeeper " + connectString);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      closeQuietly(zk);
      throw new IOException("Interrupted while connecting to ZooKeeper " + connectString, e);
    }
    return zk;
  }

  /**
   * The folders the brokers create at startup ({@code createAclPaths}); if any is missing, the ACL root is probably wrong.
   *
   * @return Missing folders (relative to the ACL root); empty if the structure is complete.
   * @throws KeeperException On ZooKeeper errors.
   * @throws InterruptedException If interrupted.
   */
  public List<String> missingStructure() throws KeeperException, InterruptedException {
    List<String> missing = new ArrayList<>();
    for (PatternType patternType : AclTree.PATTERN_TYPES) {
      checkExists(KafkaAclZkCodec.rootPath(patternType), missing);
      for (ResourceType type : KafkaAclZkCodec.resourceTypes()) {
        checkExists(KafkaAclZkCodec.typePath(patternType, type), missing);
      }
      checkExists(KafkaAclZkCodec.changeRootPath(patternType), missing);
    }
    return missing;
  }

  /**
   * Read all ACLs from ZooKeeper.
   *
   * @return The ACLs and the data nodes' versions.
   * @throws KeeperException On ZooKeeper errors.
   * @throws InterruptedException If interrupted.
   * @throws IllegalArgumentException If a data node's content is not valid ACL JSON (path in the message).
   */
  public Snapshot readCurrent() throws KeeperException, InterruptedException {
    AclTree tree = new AclTree();
    Map<ResourcePattern, Integer> versions = new HashMap<>();
    for (PatternType patternType : AclTree.PATTERN_TYPES) {
      for (ResourceType type : KafkaAclZkCodec.resourceTypes()) {
        String typePath = KafkaAclZkCodec.typePath(patternType, type);
        List<String> names;
        try {
          names = _zk.getChildren(typePath, false);
        } catch (KeeperException.NoNodeException e) {
          continue;
        }
        for (String name : names) {
          ResourcePattern pattern = new ResourcePattern(type, name, patternType);
          Stat stat = new Stat();
          byte[] data;
          try {
            data = _zk.getData(KafkaAclZkCodec.dataPath(pattern), false, stat);
          } catch (KeeperException.NoNodeException e) {
            continue;
          }
          versions.put(pattern, stat.getVersion());
          SortedSet<AclEntryKey> entries;
          try {
            entries = data == null ? Collections.emptySortedSet() : KafkaAclZkCodec.decode(data);
          } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(KafkaAclZkCodec.dataPath(pattern) + ": " + e.getMessage(), e);
          }
          if (!entries.isEmpty()) {
            tree.put(pattern, entries);
          }
        }
      }
    }
    return new Snapshot(tree, versions);
  }

  /**
   * @return The state node and its version, if it exists.
   * @throws KeeperException On ZooKeeper errors.
   * @throws InterruptedException If interrupted.
   */
  public Optional<VersionedState> readState() throws KeeperException, InterruptedException {
    Stat stat = new Stat();
    try {
      byte[] data = _zk.getData(STATE_NODE, false, stat);
      return Optional.of(new VersionedState(AclStateNode.fromBytes(data), stat.getVersion()));
    } catch (KeeperException.NoNodeException e) {
      return Optional.empty();
    }
  }

  /**
   * Take the lock (an ephemeral node, released automatically if this session ends).
   *
   * @param owner Who holds it (e.g. host name and process), stored in the node.
   * @return {@code true} if the lock was taken; {@code false} if someone else holds it.
   * @throws KeeperException On ZooKeeper errors.
   * @throws InterruptedException If interrupted.
   */
  public boolean tryLock(String owner) throws KeeperException, InterruptedException {
    ensureStateFolder();
    try {
      _zk.create(LOCK_NODE, owner.getBytes(StandardCharsets.UTF_8), _nodeAcls, CreateMode.EPHEMERAL);
      return true;
    } catch (KeeperException.NodeExistsException e) {
      return false;
    }
  }

  /**
   * Release the lock if this session holds it.
   *
   * @throws KeeperException On ZooKeeper errors.
   * @throws InterruptedException If interrupted.
   */
  public void unlock() throws KeeperException, InterruptedException {
    Stat stat = _zk.exists(LOCK_NODE, false);
    if (stat != null && stat.getEphemeralOwner() == _zk.getSessionId()) {
      try {
        _zk.delete(LOCK_NODE, stat.getVersion());
      } catch (KeeperException.NoNodeException e) {
        LOG.debug("Lock node already gone.");
      }
    }
  }

  /**
   * Apply a diff to ZooKeeper and record the new state.
   *
   * @param changes Changes from {@link AclTreeDiff#diff} (current = {@code snapshot.tree()}).
   * @param snapshot The snapshot the changes were computed from (versions for the conditional writes).
   * @param state The new state node content.
   * @param stateVersion Version of the existing state node, or empty if there is none yet.
   * @return Number of batches, operations and notifications.
   * @throws AclZkConflictException If a batch failed (nothing of that batch applied); start over.
   * @throws InterruptedException If interrupted.
   */
  public ApplyResult apply(List<ResourceChange> changes, Snapshot snapshot, AclStateNode state, Optional<Integer> stateVersion)
      throws AclZkConflictException, InterruptedException {
    return apply(changes, snapshot, state, stateVersion, Integer.MAX_VALUE);
  }

  /**
   * Same as {@link #apply(List, Snapshot, AclStateNode, Optional)}, but stops after {@code maxBatchesToRun} batches,
   * as if the process crashed there. For tests.
   *
   * @param changes Changes.
   * @param snapshot Snapshot.
   * @param state New state.
   * @param stateVersion Version of the existing state node.
   * @param maxBatchesToRun Batches to commit before stopping.
   * @return What was done.
   * @throws AclZkConflictException If a batch failed.
   * @throws InterruptedException If interrupted.
   */
  ApplyResult apply(List<ResourceChange> changes, Snapshot snapshot, AclStateNode state, Optional<Integer> stateVersion,
                    int maxBatchesToRun) throws AclZkConflictException, InterruptedException {
    try {
      ensureStateFolder();
    } catch (KeeperException e) {
      throw new AclZkConflictException("Can't create " + STATE_FOLDER + ": " + e.getMessage(), 0, e);
    }
    List<Batch> batches = buildBatches(changes, snapshot, state, stateVersion);
    int operations = 0;
    int notifications = 0;
    int committed = 0;
    for (Batch batch : batches) {
      if (committed >= maxBatchesToRun) {
        break;
      }
      try {
        _zk.multi(batch.ops());
      } catch (KeeperException e) {
        throw new AclZkConflictException(String.format("ZooKeeper batch %d of %d failed (%s at %s); it was not applied.",
                                                       committed + 1, batches.size(), e.code(), failedPath(e, batch.ops())),
                                         committed, e);
      }
      committed++;
      operations += batch.ops().size();
      notifications += batch.resources();
    }
    LOG.info("Applied {} of {} ACL change(s) in {} ZooKeeper batch(es), {} operation(s).", notifications, changes.size(),
             committed, operations);
    return new ApplyResult(committed, operations, notifications);
  }

  /**
   * Create change notifications only (no data change), so that brokers reload these resources from ZooKeeper. Used when
   * brokers serve ACLs that differ from ZooKeeper, e.g. after a data node was edited without a notification.
   *
   * @param patterns Resources to notify.
   * @return Number of notifications created.
   * @throws KeeperException On ZooKeeper errors (notifications of earlier batches stay; repeating is harmless).
   * @throws InterruptedException If interrupted.
   */
  public int notifyResources(Collection<ResourcePattern> patterns) throws KeeperException, InterruptedException {
    List<Op> ops = new ArrayList<>();
    int bytes = 0;
    int created = 0;
    for (ResourcePattern pattern : patterns) {
      KafkaAclZkCodec.ChangeNode notification = KafkaAclZkCodec.changeNode(pattern);
      byte[] data = notification.data();
      int size = estimate(notification.pathPrefix(), data);
      if (bytes + size > _maxBatchBytes && !ops.isEmpty()) {
        _zk.multi(ops);
        created += ops.size();
        ops = new ArrayList<>();
        bytes = 0;
      }
      ops.add(Op.create(notification.pathPrefix(), data, _nodeAcls, CreateMode.PERSISTENT_SEQUENTIAL));
      bytes += size;
    }
    if (!ops.isEmpty()) {
      _zk.multi(ops);
      created += ops.size();
    }
    return created;
  }

  /**
   * @return Whether the ZooKeeper session can still be used (not closed, not expired).
   */
  public boolean isAlive() {
    return _zk.getState().isAlive();
  }

  /** One ZooKeeper multi request and the number of resources (= change notifications) in it. */
  static final class Batch {
    private final List<Op> _ops = new ArrayList<>();
    private int _bytes;
    private int _resources;

    List<Op> ops() {
      return _ops;
    }

    int resources() {
      return _resources;
    }

    int bytes() {
      return _bytes;
    }

    void add(List<Op> ops, int bytes, int resources) {
      _ops.addAll(ops);
      _bytes += bytes;
      _resources += resources;
    }
  }

  /**
   * Build the batches: per resource [data-node op, change notification], packed into batches up to the size limit; the
   * state-node op goes last (in the last batch, or a batch of its own if the last one is full).
   *
   * @param changes Changes.
   * @param snapshot Snapshot.
   * @param state New state.
   * @param stateVersion Version of the existing state node.
   * @return The batches, in order.
   */
  List<Batch> buildBatches(List<ResourceChange> changes, Snapshot snapshot, AclStateNode state, Optional<Integer> stateVersion) {
    List<Batch> batches = new ArrayList<>();
    Batch batch = new Batch();
    for (ResourceChange change : changes) {
      ResourcePattern pattern = change.pattern();
      String path = KafkaAclZkCodec.dataPath(pattern);
      Optional<Integer> version = snapshot.version(pattern);
      byte[] data = change.type() == ChangeType.DELETED ? new byte[0] : KafkaAclZkCodec.encode(change.after());
      Op dataOp;
      if (change.type() == ChangeType.DELETED) {
        dataOp = Op.delete(path, version.orElseThrow(() -> new IllegalStateException("No version for deleted " + path)));
      } else if (version.isPresent()) {
        // UPDATED, or ADDED over an existing node without entries
        dataOp = Op.setData(path, data, version.get());
      } else {
        dataOp = Op.create(path, data, _nodeAcls, CreateMode.PERSISTENT);
      }
      KafkaAclZkCodec.ChangeNode notification = KafkaAclZkCodec.changeNode(pattern);
      byte[] notificationData = notification.data();
      Op notificationOp = Op.create(notification.pathPrefix(), notificationData, _nodeAcls, CreateMode.PERSISTENT_SEQUENTIAL);
      int bytes = estimate(path, data) + estimate(notification.pathPrefix(), notificationData);
      if (bytes > _maxBatchBytes) {
        throw new IllegalArgumentException(String.format("%s alone needs about %d bytes, more than the batch limit %d.",
                                                         AclTreeParser.location(pattern), bytes, _maxBatchBytes));
      }
      if (batch.bytes() + bytes > _maxBatchBytes && !batch.ops().isEmpty()) {
        batches.add(batch);
        batch = new Batch();
      }
      batch.add(List.of(dataOp, notificationOp), bytes, 1);
    }
    byte[] stateBytes = state.toBytes();
    Op stateOp = stateVersion.isPresent() ? Op.setData(STATE_NODE, stateBytes, stateVersion.get())
                                          : Op.create(STATE_NODE, stateBytes, _nodeAcls, CreateMode.PERSISTENT);
    int stateBytesEstimate = estimate(STATE_NODE, stateBytes);
    if (batch.bytes() + stateBytesEstimate > _maxBatchBytes && !batch.ops().isEmpty()) {
      batches.add(batch);
      batch = new Batch();
    }
    batch.add(List.of(stateOp), stateBytesEstimate, 0);
    batches.add(batch);
    return batches;
  }

  private static int estimate(String path, byte[] data) {
    return path.getBytes(StandardCharsets.UTF_8).length + data.length + OP_OVERHEAD_BYTES;
  }

  private static String failedPath(KeeperException e, List<Op> ops) {
    List<OpResult> results = e.getResults();
    if (results != null) {
      for (int i = 0; i < results.size() && i < ops.size(); i++) {
        if (results.get(i) instanceof OpResult.ErrorResult) {
          int err = ((OpResult.ErrorResult) results.get(i)).getErr();
          if (err != KeeperException.Code.OK.intValue() && err != KeeperException.Code.RUNTIMEINCONSISTENCY.intValue()) {
            return ops.get(i).getPath();
          }
        }
      }
    }
    return e.getPath() == null ? "?" : e.getPath();
  }

  private void ensureStateFolder() throws KeeperException, InterruptedException {
    if (_zk.exists(STATE_FOLDER, false) == null) {
      try {
        _zk.create(STATE_FOLDER, new byte[0], _nodeAcls, CreateMode.PERSISTENT);
      } catch (KeeperException.NodeExistsException e) {
        LOG.debug("{} created concurrently.", STATE_FOLDER);
      }
    }
  }

  private void checkExists(String path, List<String> missing) throws KeeperException, InterruptedException {
    if (_zk.exists(path, false) == null) {
      missing.add(path);
    }
  }

  private static void closeQuietly(ZooKeeper zk) {
    try {
      zk.close();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public void close() {
    closeQuietly(_zk);
  }
}
