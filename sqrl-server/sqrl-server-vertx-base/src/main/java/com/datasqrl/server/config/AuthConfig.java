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
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * The {@code auth} block of the server config: the issuers whose tokens this server trusts. Each
 * issuer is validated with its own keys, audiences and leeway.
 *
 * @param issuers the trusted issuers, never empty
 * @param resource the public URL of the MCP endpoint for RFC 9728 metadata, or null to derive it
 *     from each request
 * @param scopesSupported the scopes advertised to MCP clients
 */
public record AuthConfig(List<Issuer> issuers, String resource, List<String> scopesSupported) {

  private static final int DEFAULT_LEEWAY_SECONDS = 60;
  public static final List<String> DEFAULT_SCOPES = List.of("mcp:tools", "mcp:resources");

  private static final Set<String> SUPPORTED_ALGORITHMS =
      Set.of(
          "HS256", "HS384", "HS512", "RS256", "RS384", "RS512", "PS256", "PS384", "PS512", "ES256",
          "ES384", "ES512", "EdDSA");

  private static final Set<String> AUTH_KEYS =
      Set.of("issuers", "leeway", "algorithms", "resource", "scopesSupported");
  private static final Set<String> ISSUER_KEYS =
      Set.of("issuer", "audiences", "keys", "jwksUri", "leeway", "algorithms");

  /**
   * One trusted issuer. At most one of {@code keys} and {@code jwksUri} is set; with neither, the
   * keys come from OIDC discovery of {@code issuer}.
   *
   * @param issuer the exact {@code iss} value
   * @param audiences the accepted audiences, never empty
   * @param keys inline JWKs, or null
   * @param jwksUri the JWK Set URL, or null
   * @param leeway the allowed clock skew in seconds
   * @param algorithms the allowed signature algorithms, or null for no restriction
   */
  public record Issuer(
      String issuer,
      List<String> audiences,
      List<JsonObject> keys,
      String jwksUri,
      int leeway,
      List<String> algorithms) {

    /**
     * Applies {@code algorithms} to the issuer's keys. Without it, the keys are returned as they
     * are, and each key verifies tokens signed with its own {@code alg}, or with the default for
     * its {@code kty} when it has none. With it, a key with an {@code alg} is kept only if that
     * algorithm is listed, and a key without one is used once for each listed algorithm its {@code
     * kty} supports.
     */
    public List<JsonObject> keysForAlgorithms(List<JsonObject> keys) {
      return algorithms == null
          ? keys
          : keys.stream().flatMap(key -> withAllowedAlgorithms(key, algorithms)).toList();
    }
  }

  /**
   * Returns the issuers to advertise as RFC 9728 authorization servers. Issuers with inline {@code
   * keys} (a backend or a test secret) and issuers whose name is not a URL are left out: they are
   * not OAuth servers an MCP client could use.
   */
  public List<String> authorizationServers() {
    return issuers.stream()
        .filter(issuer -> issuer.keys() == null && isHttpUrlOrPlaceholder(issuer.issuer()))
        .map(Issuer::issuer)
        .toList();
  }

  public static AuthConfig parse(JsonObject json) {
    checkKnownKeys("auth", json, AUTH_KEYS);
    var leeway = leeway("auth", json, DEFAULT_LEEWAY_SECONDS);
    var algorithms = algorithms("auth", json, null);

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
                seen.add(issuer.issuer()),
                "auth.issuers lists issuer '%s' more than once",
                issuer.issuer()));

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

    var jwksUri = optionalString(path + ".jwksUri", json, "jwksUri");
    List<JsonObject> keys = null;
    if (json.getValue("keys") != null) {
      checkArgument(
          json.getValue("keys") instanceof JsonArray array
              && !array.isEmpty()
              && array.stream().allMatch(JsonObject.class::isInstance),
          "%s.keys must be a non-empty array of JWK objects",
          path);
      keys = json.getJsonArray("keys").stream().map(JsonObject.class::cast).toList();
    } else if (jwksUri != null) {
      checkArgument(
          isHttpUrlOrPlaceholder(jwksUri), "%s.jwksUri must be an absolute http(s) URL", path);
    } else {
      checkArgument(
          isHttpUrlOrPlaceholder(issuer),
          "%s.issuer must be an absolute http(s) URL to discover its keys, or set 'keys' or"
              + " 'jwksUri'",
          path);
    }

    var parsed =
        new Issuer(
            issuer,
            audiences,
            keys,
            jwksUri,
            leeway(path, json, defaultLeeway),
            algorithms(path, json, defaultAlgorithms));

    checkArgument(
        keys == null
            || parsed.keysForAlgorithms(keys).stream().anyMatch(AuthConfig::canVerifySignatures),
        "%s.keys has no key that can verify signatures, so the server would accept unsigned"
            + " tokens. A key needs its public material (RSA: n and e, EC: crv, x and y, OKP: x,"
            + " oct: k), its \"use\" must be sig if set, and it must fit 'algorithms'",
        path);

    return parsed;
  }

  /**
   * Whether a JWK can verify signatures: it is not marked for another use, and it carries the
   * public material of its key type. Values are not decoded, so a {@code ${...}} placeholder
   * counts. Keys that only fail once decoded are caught at startup.
   */
  static boolean canVerifySignatures(JsonObject jwk) {
    var use = jwk.getString("use");
    if (use != null && !"sig".equals(use)) {
      return false;
    }

    var kty = jwk.getString("kty");
    kty = kty == null ? "" : kty;

    return switch (kty) {
      case "RSA", "RSASSA" ->
          jwk.containsKey("x5c") || (jwk.containsKey("n") && jwk.containsKey("e"));
      case "EC" -> jwk.containsKey("crv") && jwk.containsKey("x") && jwk.containsKey("y");
      case "OKP" -> jwk.containsKey("x");
      case "oct" -> jwk.containsKey("k");
      default -> false;
    };
  }

  /**
   * Whether the value is an absolute http(s) URL or still holds a {@code ${...}} placeholder. A
   * placeholder passes so that a compile without the variable succeeds. At startup a resolved value
   * gets the real check, and a variable that is still unset fails when its keys are fetched.
   */
  private static boolean isHttpUrlOrPlaceholder(String value) {
    if (value.contains("${")) {
      return true;
    }
    try {
      var uri = new URI(value);
      return ("http".equals(uri.getScheme()) || "https".equals(uri.getScheme()))
          && uri.getHost() != null;
    } catch (URISyntaxException e) {
      return false;
    }
  }

  private static Stream<JsonObject> withAllowedAlgorithms(JsonObject key, List<String> algorithms) {
    var kty = key.getString("kty");
    var alg = key.getString("alg");
    if (alg != null) {
      return algorithms.contains(alg) ? Stream.of(key) : Stream.empty();
    }
    return algorithms.stream()
        .filter(candidate -> fitsKeyType(kty, candidate))
        .map(candidate -> key.copy().put("alg", candidate));
  }

  private static boolean fitsKeyType(String kty, String alg) {
    return switch (kty == null ? "" : kty) {
      case "RSA" -> alg.startsWith("RS") || alg.startsWith("PS");
      case "EC" -> alg.startsWith("ES");
      case "oct" -> alg.startsWith("HS");
      case "OKP" -> alg.equals("EdDSA");
      default -> false;
    };
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

  private static int leeway(String path, JsonObject json, int defaultLeeway) {
    var value = json.getValue("leeway");
    if (value == null) {
      return defaultLeeway;
    }
    checkArgument(value instanceof Integer, "%s.leeway must be an integer number of seconds", path);
    checkArgument((Integer) value >= 0, "%s.leeway must not be negative", path);
    return (Integer) value;
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
