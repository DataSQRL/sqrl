/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasqrl.server.config;

import static com.google.common.base.Preconditions.checkArgument;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.IntStream;
import lombok.Value;

/**
 * The {@code auth} block of the server config: the issuers whose tokens this server trusts. Each
 * issuer is validated with its own keys, audiences, leeway and algorithms.
 */
@Value
public class AuthConfig {

  public static final int DEFAULT_LEEWAY_SECONDS = 60;
  public static final List<String> DEFAULT_ALGORITHMS = List.of("RS256");
  public static final List<String> DEFAULT_SCOPES = List.of("mcp:tools", "mcp:resources");

  static final Set<String> SUPPORTED_ALGORITHMS =
      Set.of(
          "HS256", "HS384", "HS512", "RS256", "RS384", "RS512", "PS256", "PS384", "PS512", "ES256",
          "ES384", "ES512", "EdDSA");

  private static final Set<String> AUTH_KEYS =
      Set.of("issuers", "leeway", "algorithms", "resource", "scopesSupported");
  private static final Set<String> ISSUER_KEYS =
      Set.of("issuer", "audiences", "keys", "jwksUri", "leeway", "algorithms");

  List<Issuer> issuers;
  String resource;
  List<String> scopesSupported;

  @Value
  public static class Issuer {
    String issuer;
    List<String> audiences;
    List<JsonObject> keys;
    String jwksUri;
    int leeway;
    List<String> algorithms;
  }

  public List<String> getAuthorizationServers() {
    return issuers.stream().map(Issuer::getIssuer).toList();
  }

  public static AuthConfig parse(JsonObject json) {
    checkKnownKeys("auth", json, AUTH_KEYS);
    var leeway = leeway("auth", json, DEFAULT_LEEWAY_SECONDS);
    var algorithms = algorithms("auth", json, DEFAULT_ALGORITHMS);

    checkArgument(
        json.getValue("issuers") instanceof JsonArray array && !array.isEmpty(),
        "auth.issuers must be a non-empty array");
    var issuerArray = json.getJsonArray("issuers");
    var issuers =
        IntStream.range(0, issuerArray.size())
            .mapToObj(
                i ->
                    parseIssuer(
                        "auth.issuers[" + i + "]", issuerArray.getValue(i), leeway, algorithms))
            .toList();

    var seen = new HashSet<String>();
    issuers.forEach(
        issuer ->
            checkArgument(
                seen.add(issuer.getIssuer()),
                "auth.issuers lists issuer '%s' more than once",
                issuer.getIssuer()));

    var scopes = stringList("auth.scopesSupported", json.getValue("scopesSupported"), true);
    return new AuthConfig(
        issuers,
        optionalString("auth.resource", json, "resource"),
        scopes == null ? DEFAULT_SCOPES : scopes);
  }

  private static Issuer parseIssuer(
      String path, Object value, int defaultLeeway, List<String> defaultAlgorithms) {
    checkArgument(value instanceof JsonObject, "%s must be a JSON object", path);
    var json = (JsonObject) value;
    checkKnownKeys(path, json, ISSUER_KEYS);

    var issuer = optionalString(path + ".issuer", json, "issuer");
    checkArgument(issuer != null, "%s.issuer is required", path);
    var audiences = stringList(path + ".audiences", json.getValue("audiences"), false);
    checkArgument(audiences != null, "%s.audiences is required", path);
    checkArgument(
        json.getValue("keys") == null || json.getValue("jwksUri") == null,
        "%s must set at most one of 'keys' and 'jwksUri'",
        path);

    List<JsonObject> keys = null;
    if (json.getValue("keys") != null) {
      checkArgument(
          json.getValue("keys") instanceof JsonArray array
              && !array.isEmpty()
              && array.stream().allMatch(JsonObject.class::isInstance),
          "%s.keys must be a non-empty array of JWK objects",
          path);
      keys = json.getJsonArray("keys").stream().map(JsonObject.class::cast).toList();
    }

    return new Issuer(
        issuer,
        audiences,
        keys,
        optionalString(path + ".jwksUri", json, "jwksUri"),
        leeway(path, json, defaultLeeway),
        algorithms(path, json, defaultAlgorithms));
  }

  private static int leeway(String path, JsonObject json, int defaultLeeway) {
    var value = json.getValue("leeway");
    if (value == null) {
      return defaultLeeway;
    }
    checkArgument(value instanceof Integer, "%s.leeway must be an integer number of seconds", path);
    checkArgument((Integer) value >= 0, "%s.leeway must not be negative", path);
    return (Integer) value;
  }

  private static List<String> algorithms(
      String path, JsonObject json, List<String> defaultAlgorithms) {
    var algorithms = stringList(path + ".algorithms", json.getValue("algorithms"), false);
    if (algorithms == null) {
      return defaultAlgorithms;
    }
    var unsupported = new TreeSet<>(algorithms);
    unsupported.removeAll(SUPPORTED_ALGORITHMS);
    checkArgument(
        unsupported.isEmpty(),
        "%s.algorithms has unsupported value(s) %s, supported are %s",
        path,
        unsupported,
        new TreeSet<>(SUPPORTED_ALGORITHMS));
    return algorithms;
  }

  private static List<String> stringList(String path, Object value, boolean allowEmpty) {
    if (value == null) {
      return null;
    }
    checkArgument(
        value instanceof JsonArray array
            && (allowEmpty || !array.isEmpty())
            && array.stream().allMatch(item -> item instanceof String str && !str.isBlank()),
        "%s must be a %sarray of non-blank strings",
        path,
        allowEmpty ? "" : "non-empty ");
    return ((JsonArray) value).stream().map(String.class::cast).toList();
  }

  private static String optionalString(String path, JsonObject json, String key) {
    var value = json.getValue(key);
    if (value == null) {
      return null;
    }
    checkArgument(
        value instanceof String str && !str.isBlank(), "%s must be a non-blank string", path);
    return (String) value;
  }

  private static void checkKnownKeys(String path, JsonObject json, Set<String> allowed) {
    var unknown = new TreeSet<>(json.fieldNames());
    unknown.removeAll(allowed);
    checkArgument(
        unknown.isEmpty(),
        "%s has unknown key(s) %s, supported keys are %s",
        path,
        unknown,
        new TreeSet<>(allowed));
  }
}
