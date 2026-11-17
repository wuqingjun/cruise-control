/*
 * Copyright 2026 qjwu. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.acl;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;

/**
 * Content of the state node {@code <acl root>/cruise-control-acl-sync/current}: what was applied last.
 * <pre>
 * {"version": 1, "sha256": "…", "appliedAtMs": 1794620000000, "file": "/opt/kafka-acls/acls.json",
 *  "unmanaged": [{"resourceType": "TOPIC", "patternType": "LITERAL", "name": "legacy"}]}
 * </pre>
 * Only the latest version is kept (history is in version control); the comparison base is ZooKeeper itself, so the file
 * content is not stored.
 */
public final class AclStateNode {
  private static final int VERSION = 1;
  private static final Gson GSON = new Gson();
  private static final Comparator<ResourcePattern> PATTERN_ORDER = AclTree.PATTERN_ORDER;

  private final String _sha256;
  private final long _appliedAtMs;
  private final String _file;
  private final Set<ResourcePattern> _unmanaged;

  /**
   * @param sha256 SHA-256 (hex) of the applied ACL file.
   * @param appliedAtMs When it was applied.
   * @param file Path of the applied file.
   * @param unmanaged Pre-existing resources left alone (G28).
   */
  public AclStateNode(String sha256, long appliedAtMs, String file, Collection<ResourcePattern> unmanaged) {
    _sha256 = Objects.requireNonNull(sha256, "sha256");
    _appliedAtMs = appliedAtMs;
    _file = Objects.requireNonNull(file, "file");
    Set<ResourcePattern> sorted = new TreeSet<>(PATTERN_ORDER);
    sorted.addAll(unmanaged);
    _unmanaged = Collections.unmodifiableSet(sorted);
  }

  public String sha256() {
    return _sha256;
  }

  public long appliedAtMs() {
    return _appliedAtMs;
  }

  public String file() {
    return _file;
  }

  /**
   * @return Unmanaged resources, sorted in tree order (unmodifiable).
   */
  public Set<ResourcePattern> unmanaged() {
    return _unmanaged;
  }

  /**
   * @return The node's JSON content.
   */
  public byte[] toBytes() {
    JsonObject json = new JsonObject();
    json.addProperty("version", VERSION);
    json.addProperty("sha256", _sha256);
    json.addProperty("appliedAtMs", _appliedAtMs);
    json.addProperty("file", _file);
    JsonArray unmanaged = new JsonArray();
    for (ResourcePattern pattern : _unmanaged) {
      JsonObject p = new JsonObject();
      p.addProperty("resourceType", pattern.resourceType().name());
      p.addProperty("patternType", pattern.patternType().name());
      p.addProperty("name", pattern.name());
      unmanaged.add(p);
    }
    json.add("unmanaged", unmanaged);
    return GSON.toJson(json).getBytes(StandardCharsets.UTF_8);
  }

  /**
   * @param bytes The node's JSON content.
   * @return The parsed state.
   * @throws IllegalArgumentException If the content is not a valid state node.
   */
  public static AclStateNode fromBytes(byte[] bytes) {
    try {
      JsonObject json = JsonParser.parseString(new String(bytes, StandardCharsets.UTF_8)).getAsJsonObject();
      if (json.get("version").getAsInt() != VERSION) {
        throw new IllegalArgumentException("Unsupported state node version " + json.get("version"));
      }
      Set<ResourcePattern> unmanaged = new TreeSet<>(PATTERN_ORDER);
      for (JsonElement e : json.getAsJsonArray("unmanaged")) {
        JsonObject p = e.getAsJsonObject();
        unmanaged.add(new ResourcePattern(ResourceType.valueOf(p.get("resourceType").getAsString()), p.get("name").getAsString(),
                                          PatternType.valueOf(p.get("patternType").getAsString())));
      }
      return new AclStateNode(json.get("sha256").getAsString(), json.get("appliedAtMs").getAsLong(),
                              json.get("file").getAsString(), unmanaged);
    } catch (RuntimeException e) {
      throw new IllegalArgumentException("Invalid ACL sync state node: " + e.getMessage(), e);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof AclStateNode)) {
      return false;
    }
    AclStateNode that = (AclStateNode) o;
    return _appliedAtMs == that._appliedAtMs && _sha256.equals(that._sha256) && _file.equals(that._file)
           && _unmanaged.equals(that._unmanaged);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_sha256, _appliedAtMs, _file, _unmanaged);
  }

  @Override
  public String toString() {
    return String.format("AclStateNode(sha256=%s, appliedAtMs=%d, file=%s, unmanaged=%d)", _sha256, _appliedAtMs, _file,
                         _unmanaged.size());
  }
}
