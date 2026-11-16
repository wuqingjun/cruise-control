/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.acl;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedSet;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.utils.SecurityUtils;

/**
 * Parses the deployed ACL file into an {@link AclTree}. The file has no ZooKeeper root; it contains the two subtrees as
 * they appear under the root, and each leaf is a data node's content verbatim:
 * <pre>
 * {
 *   "kafka-acl": {
 *     "Topic": { "orders": {"version": 1, "acls": [{"principal": "Claim:x", "permissionType": "Allow",
 *                                                   "host": "*", "operation": "Read"}]} }
 *   },
 *   "kafka-acl-extended": {
 *     "prefixed": { "Group": { "orders-": {"version": 1, "acls": [...]} } }
 *   }
 * }
 * </pre>
 * The pattern type comes from the subtree ({@code kafka-acl} = LITERAL, {@code kafka-acl-extended/prefixed} = PREFIXED).
 * Leaves are decoded with Kafka's own parser ({@link KafkaAclZkCodec#decode}). This class checks the structure; the
 * content rules are in {@link AclValidator}.
 */
public final class AclTreeParser {
  /** Top-level key of the LITERAL subtree (ZooKeeper folder {@code /kafka-acl}). */
  public static final String LITERAL_TREE = "kafka-acl";
  /** Top-level key of the extended subtree (ZooKeeper folder {@code /kafka-acl-extended}). */
  public static final String EXTENDED_TREE = "kafka-acl-extended";
  /** The only folder under the extended subtree (PREFIXED). */
  public static final String PREFIXED_FOLDER = "prefixed";
  private static final String VERSION_KEY = "version";
  private static final String ACLS_KEY = "acls";
  private static final int SUPPORTED_VERSION = 1;
  private static final Gson GSON = new Gson();

  private AclTreeParser() {
  }

  /**
   * Parse the ACL file.
   *
   * @param content File content (UTF-8 JSON).
   * @return The ACLs as a sorted tree.
   * @throws AclFileException If the file is empty, not valid JSON, or not in the expected structure. The message names the
   * location in the file.
   */
  public static AclTree parse(byte[] content) throws AclFileException {
    if (content == null || new String(content, StandardCharsets.UTF_8).trim().isEmpty()) {
      throw new AclFileException("The ACL file is empty.");
    }
    AclTree tree = new AclTree();
    JsonReader reader = new JsonReader(new StringReader(new String(content, StandardCharsets.UTF_8)));
    String location = "(top level)";
    try {
      expect(reader, JsonToken.BEGIN_OBJECT, location);
      reader.beginObject();
      Set<String> seen = new HashSet<>();
      while (reader.hasNext()) {
        String key = reader.nextName();
        location = key;
        checkUnique(seen, key, "(top level)");
        if (LITERAL_TREE.equals(key)) {
          parseResourceTypes(reader, PatternType.LITERAL, LITERAL_TREE, tree);
        } else if (EXTENDED_TREE.equals(key)) {
          parseExtended(reader, tree);
        } else {
          throw new AclFileException(String.format("Unknown top-level key '%s'; expected '%s' or '%s'.", key, LITERAL_TREE,
                                                   EXTENDED_TREE));
        }
      }
      reader.endObject();
      location = "(end of file)";
      boolean endOfDocument;
      try {
        endOfDocument = reader.peek() == JsonToken.END_DOCUMENT;
      } catch (IOException e) {
        // A strict reader throws on any second value.
        endOfDocument = false;
      }
      if (!endOfDocument) {
        throw new AclFileException("Unexpected content after the top-level object.");
      }
    } catch (IOException | IllegalStateException | JsonParseException e) {
      throw new AclFileException(String.format("Invalid JSON at %s: %s", location, e.getMessage()), e);
    }
    return tree;
  }

  /**
   * @param pattern Resource pattern.
   * @return The pattern's location in the file (and under the ZooKeeper root), e.g. {@code kafka-acl/Topic/orders} or
   * {@code kafka-acl-extended/prefixed/Group/orders-}.
   */
  public static String location(ResourcePattern pattern) {
    return String.format("%s/%s/%s", subtreeLocation(pattern.patternType()), SecurityUtils.resourceTypeName(pattern.resourceType()),
                         pattern.name());
  }

  private static String subtreeLocation(PatternType patternType) {
    return patternType == PatternType.LITERAL ? LITERAL_TREE : EXTENDED_TREE + "/" + PREFIXED_FOLDER;
  }

  private static void parseExtended(JsonReader reader, AclTree tree) throws IOException, AclFileException {
    expect(reader, JsonToken.BEGIN_OBJECT, EXTENDED_TREE);
    reader.beginObject();
    Set<String> seen = new HashSet<>();
    while (reader.hasNext()) {
      String key = reader.nextName();
      checkUnique(seen, key, EXTENDED_TREE);
      if (!PREFIXED_FOLDER.equals(key)) {
        throw new AclFileException(String.format("%s: unknown folder '%s'; the only folder is '%s'.", EXTENDED_TREE, key,
                                                 PREFIXED_FOLDER));
      }
      parseResourceTypes(reader, PatternType.PREFIXED, EXTENDED_TREE + "/" + PREFIXED_FOLDER, tree);
    }
    reader.endObject();
  }

  private static void parseResourceTypes(JsonReader reader, PatternType patternType, String location, AclTree tree)
      throws IOException, AclFileException {
    expect(reader, JsonToken.BEGIN_OBJECT, location);
    reader.beginObject();
    Set<String> seen = new HashSet<>();
    while (reader.hasNext()) {
      String typeName = reader.nextName();
      checkUnique(seen, typeName, location);
      ResourceType resourceType = SecurityUtils.resourceType(typeName);
      if (resourceType == ResourceType.UNKNOWN || resourceType == ResourceType.ANY
          || !SecurityUtils.resourceTypeName(resourceType).equals(typeName)) {
        throw new AclFileException(String.format("%s: unknown resource type '%s'; expected one of %s (exact spelling).", location,
                                                 typeName, resourceTypeNames()));
      }
      parseResources(reader, new ResourcePattern(resourceType, "x", patternType), location + "/" + typeName, tree);
    }
    reader.endObject();
  }

  private static void parseResources(JsonReader reader, ResourcePattern typeTemplate, String location, AclTree tree)
      throws IOException, AclFileException {
    expect(reader, JsonToken.BEGIN_OBJECT, location);
    reader.beginObject();
    Set<String> seen = new HashSet<>();
    while (reader.hasNext()) {
      String name = reader.nextName();
      checkUnique(seen, name, location);
      ResourcePattern pattern = new ResourcePattern(typeTemplate.resourceType(), name, typeTemplate.patternType());
      JsonElement leaf = JsonParser.parseReader(reader);
      tree.put(pattern, parseLeaf(leaf, location(pattern)));
    }
    reader.endObject();
  }

  private static SortedSet<AclEntryKey> parseLeaf(JsonElement leaf, String location) throws AclFileException {
    if (!leaf.isJsonObject()) {
      throw new AclFileException(String.format("%s: expected {\"version\": 1, \"acls\": [...]}.", location));
    }
    JsonObject node = leaf.getAsJsonObject();
    JsonElement version = node.get(VERSION_KEY);
    if (!(version instanceof JsonPrimitive) || !((JsonPrimitive) version).isNumber()
        || version.getAsDouble() != SUPPORTED_VERSION) {
      throw new AclFileException(String.format("%s: '%s' must be %d.", location, VERSION_KEY, SUPPORTED_VERSION));
    }
    JsonElement acls = node.get(ACLS_KEY);
    if (!(acls instanceof JsonArray)) {
      throw new AclFileException(String.format("%s: '%s' must be a list of ACL entries.", location, ACLS_KEY));
    }
    int rawCount = ((JsonArray) acls).size();
    if (rawCount == 0) {
      throw new AclFileException(String.format("%s: no ACL entries; remove the resource instead.", location));
    }
    SortedSet<AclEntryKey> entries;
    try {
      entries = KafkaAclZkCodec.decode(GSON.toJson(node).getBytes(StandardCharsets.UTF_8));
    } catch (IllegalArgumentException e) {
      throw new AclFileException(String.format("%s: invalid ACL entry: %s", location, e.getMessage()), e);
    }
    if (entries.size() != rawCount) {
      throw new AclFileException(String.format("%s: %d duplicate ACL entr%s.", location, rawCount - entries.size(),
                                               rawCount - entries.size() == 1 ? "y" : "ies"));
    }
    return entries;
  }

  private static void expect(JsonReader reader, JsonToken token, String location) throws IOException, AclFileException {
    JsonToken actual = reader.peek();
    if (actual != token) {
      throw new AclFileException(String.format("%s: expected a JSON object but found %s.", location, actual));
    }
  }

  private static void checkUnique(Set<String> seen, String key, String location) throws AclFileException {
    if (!seen.add(key)) {
      throw new AclFileException(String.format("%s: duplicate key '%s'.", location, key));
    }
  }

  private static String resourceTypeNames() {
    StringBuilder sb = new StringBuilder();
    KafkaAclZkCodec.resourceTypes().stream().sorted(AclTree.RESOURCE_TYPE_ORDER).forEach(t -> {
      if (sb.length() > 0) {
        sb.append(", ");
      }
      sb.append(SecurityUtils.resourceTypeName(t));
    });
    return sb.toString();
  }
}
