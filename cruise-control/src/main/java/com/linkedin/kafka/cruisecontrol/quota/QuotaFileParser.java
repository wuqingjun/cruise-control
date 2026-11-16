/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.quota;

import com.google.gson.JsonParseException;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.linkedin.kafka.cruisecontrol.acl.AclFileException;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.SecurityUtils;

/**
 * Parses and validates the quota file:
 * <pre>
 * {
 *   "version": 1,
 *   "default": { "producer_byte_rate": 1048576, "consumer_byte_rate": 2097152, "request_percentage": 5 },
 *   "quotas": {
 *     "Claim:payments-team": { "producer_byte_rate": 52428800, "consumer_byte_rate": 104857600 },
 *     "User:O=LocalDev,CN=cruise-control": { "producer_byte_rate": 104857600 }
 *   }
 * }
 * </pre>
 * Strict: unknown or duplicate keys, a missing or empty {@code default}, values that aren't positive finite numbers,
 * identities that aren't {@code Type:name} with an allowed type → the whole file is rejected (nothing is applied). All
 * problems are reported at once.
 */
public final class QuotaFileParser {
  static final String VERSION = "version";
  static final String DEFAULT = "default";
  static final String QUOTAS = "quotas";

  private QuotaFileParser() {
  }

  /**
   * @param content File content (UTF-8 JSON).
   * @param principalTypes Allowed identity types, e.g. {@code User}, {@code Claim}, {@code Certificate}.
   * @return The quotas, the default under {@link QuotaSet#DEFAULT}.
   * @throws AclFileException If the file can't be used; the message lists every problem with its location.
   */
  public static QuotaSet parse(byte[] content, Collection<String> principalTypes) throws AclFileException {
    if (content == null || new String(content, StandardCharsets.UTF_8).trim().isEmpty()) {
      throw new AclFileException("The quota file is empty.");
    }
    Map<String, Map<String, Double>> quotas = new LinkedHashMap<>();
    List<String> problems = new ArrayList<>();
    String location = "(top level)";
    try (JsonReader reader = new JsonReader(new StringReader(new String(content, StandardCharsets.UTF_8)))) {
      expect(reader, JsonToken.BEGIN_OBJECT, location);
      reader.beginObject();
      Set<String> seen = new HashSet<>();
      while (reader.hasNext()) {
        String key = reader.nextName();
        location = key;
        if (!seen.add(key)) {
          throw new AclFileException("Duplicate top-level key '" + key + "'.");
        }
        switch (key) {
          case VERSION:
            expect(reader, JsonToken.NUMBER, VERSION);
            if (reader.nextDouble() != 1) {
              throw new AclFileException("Unsupported quota file version; expected 1.");
            }
            break;
          case DEFAULT:
            quotas.put(QuotaSet.DEFAULT, parseValues(reader, DEFAULT, problems));
            break;
          case QUOTAS:
            parseQuotas(reader, quotas, principalTypes, problems);
            break;
          default:
            throw new AclFileException(String.format("Unknown top-level key '%s'; expected '%s', '%s' or '%s'.", key, VERSION,
                                                     DEFAULT, QUOTAS));
        }
      }
      reader.endObject();
      location = "(end of file)";
      if (!endOfDocument(reader)) {
        throw new AclFileException("Unexpected content after the top-level object.");
      }
      if (!seen.contains(VERSION)) {
        problems.add("Missing 'version' (1).");
      }
      if (quotas.getOrDefault(QuotaSet.DEFAULT, Map.of()).isEmpty()) {
        problems.add("Missing or empty 'default': principals without a matching identity need a (low) default quota.");
      }
    } catch (IOException | IllegalStateException | JsonParseException e) {
      throw new AclFileException(String.format("Invalid JSON at %s: %s", location, e.getMessage()), e);
    }
    if (!problems.isEmpty()) {
      throw new AclFileException(String.format("%d problem(s) in the quota file: %s", problems.size(), String.join(" ", problems)));
    }
    return new QuotaSet(quotas);
  }

  private static void parseQuotas(JsonReader reader, Map<String, Map<String, Double>> quotas, Collection<String> principalTypes,
                                  List<String> problems) throws IOException, AclFileException {
    expect(reader, JsonToken.BEGIN_OBJECT, QUOTAS);
    reader.beginObject();
    while (reader.hasNext()) {
      String identity = reader.nextName();
      String location = QUOTAS + "/" + identity;
      if (quotas.containsKey(identity)) {
        throw new AclFileException("Duplicate identity " + location + ".");
      }
      validateIdentity(identity, location, principalTypes, problems);
      Map<String, Double> values = parseValues(reader, location, problems);
      if (values.isEmpty()) {
        problems.add(location + ": no quota values.");
      }
      quotas.put(identity, values);
    }
    reader.endObject();
  }

  private static Map<String, Double> parseValues(JsonReader reader, String location, List<String> problems)
      throws IOException, AclFileException {
    expect(reader, JsonToken.BEGIN_OBJECT, location);
    Map<String, Double> values = new LinkedHashMap<>();
    reader.beginObject();
    while (reader.hasNext()) {
      String key = reader.nextName();
      if (values.containsKey(key)) {
        throw new AclFileException(String.format("Duplicate key '%s' in %s.", key, location));
      }
      expect(reader, JsonToken.NUMBER, location + "/" + key);
      double value = reader.nextDouble();
      if (!QuotaSet.QUOTA_KEYS.contains(key)) {
        problems.add(String.format("%s: unknown quota '%s' (allowed: %s).", location, key, QuotaSet.QUOTA_KEYS));
      } else if (!(value > 0) || Double.isInfinite(value)) {
        problems.add(String.format("%s/%s: must be a positive number.", location, key));
      } else {
        values.put(key, value);
      }
    }
    reader.endObject();
    return values;
  }

  private static void validateIdentity(String identity, String location, Collection<String> principalTypes, List<String> problems) {
    try {
      KafkaPrincipal principal = SecurityUtils.parseKafkaPrincipal(identity);
      if (!principalTypes.contains(principal.getPrincipalType())) {
        problems.add(String.format("%s: identity type '%s' is not allowed (allowed: %s).", location, principal.getPrincipalType(),
                                   principalTypes));
      }
      if (principal.getName().isEmpty()) {
        problems.add(location + ": identity name must not be empty.");
      }
    } catch (IllegalArgumentException e) {
      problems.add(location + ": identity must be 'Type:name'.");
    }
  }

  private static void expect(JsonReader reader, JsonToken token, String location) throws IOException, AclFileException {
    JsonToken actual = reader.peek();
    if (actual != token) {
      throw new AclFileException(String.format("%s: expected %s, found %s.", location, token, actual));
    }
  }

  private static boolean endOfDocument(JsonReader reader) {
    try {
      return reader.peek() == JsonToken.END_DOCUMENT;
    } catch (IOException e) {
      // A strict reader throws on any second value.
      return false;
    }
  }
}
