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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.vertx.core.json.JsonObject;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class AuthConfigTest {

  private static final Map<String, Object> KEY = Map.of("kty", "RSA", "n", "AQAB", "e", "AQAB");

  @Test
  void givenMinimalIssuer_whenParsed_thenDefaultsApply() {
    var authConfig = parse(Map.of("issuers", List.of(issuer("https://a/"))));

    var issuer = authConfig.issuers().get(0);
    assertThat(issuer.leeway()).isEqualTo(60);
    assertThat(issuer.algorithms()).as("no algorithm restriction").isNull();
    assertThat(issuer.keys()).isNull();
    assertThat(issuer.jwksUri()).isNull();
    assertThat(authConfig.scopesSupported()).containsExactly("mcp:tools", "mcp:resources");
    assertThat(authConfig.resource()).isNull();
  }

  @Test
  void givenAuthLevelDefaultsAndIssuerOverrides_whenParsed_thenIssuerValueWins() {
    var overriding = issuer("https://b/");
    overriding.put("leeway", 5);
    overriding.put("algorithms", List.of("ES256"));

    var authConfig =
        parse(
            Map.of(
                "leeway",
                30,
                "algorithms",
                List.of("PS256"),
                "issuers",
                List.of(issuer("https://a/"), overriding)));

    assertThat(authConfig.issuers()).extracting(AuthConfig.Issuer::leeway).containsExactly(30, 5);
    assertThat(authConfig.issuers())
        .extracting(AuthConfig.Issuer::algorithms)
        .containsExactly(List.of("PS256"), List.of("ES256"));
  }

  @Test
  void givenAlgorithms_whenKeysApplied_thenKeysWithoutAlgUseEachListedAlgorithmOfTheirType() {
    var rsaWithoutAlg = new JsonObject(KEY);
    var ecWithUnlistedAlg = new JsonObject().put("kty", "EC").put("alg", "ES256");
    var restricted = issuerWithAlgorithms(List.of("RS256", "PS256", "HS256"));

    assertThat(restricted.keysForAlgorithms(List.of(rsaWithoutAlg, ecWithUnlistedAlg)))
        .extracting(key -> key.getString("alg"))
        .containsExactly("RS256", "PS256");
    assertThat(issuerWithAlgorithms(null).keysForAlgorithms(List.of(rsaWithoutAlg)))
        .as("no restriction")
        .containsExactly(rsaWithoutAlg);
  }

  @Test
  void givenIssuersWithEachKeySource_whenParsed_thenInlineKeyIssuersAreNotAuthorizationServers() {
    var inlineKeys = issuer("my-backend");
    inlineKeys.put("keys", List.of(KEY));
    var jwksUri = issuer("https://c/");
    jwksUri.put("jwksUri", "https://c/jwks");

    var authConfig =
        parse(
            Map.of(
                "issuers",
                List.of(issuer("https://a/"), inlineKeys, jwksUri, issuer("https://b/")),
                "resource",
                "https://api.example.com/v1/mcp",
                "scopesSupported",
                List.of("mcp:tools")));

    assertThat(authConfig.authorizationServers())
        .containsExactly("https://a/", "https://c/", "https://b/");
    assertThat(authConfig.resource()).isEqualTo("https://api.example.com/v1/mcp");
    assertThat(authConfig.scopesSupported()).containsExactly("mcp:tools");
  }

  @Test
  void givenUnresolvedPlaceholders_whenParsed_thenUrlChecksAreLeftToStartup() {
    var jwksUri = issuer("${UNSET_AUTH_TEST_ISSUER_B}");
    jwksUri.put("jwksUri", "${UNSET_AUTH_TEST_JWKS_URI}");

    var authConfig =
        parse(Map.of("issuers", List.of(issuer("${UNSET_AUTH_TEST_ISSUER_A}"), jwksUri)));

    assertThat(authConfig.issuers())
        .extracting(AuthConfig.Issuer::issuer)
        .containsExactly("${UNSET_AUTH_TEST_ISSUER_A}", "${UNSET_AUTH_TEST_ISSUER_B}");
  }

  static List<Arguments> invalidConfigs() {
    var noIssuer = issuer("https://a/");
    noIssuer.remove("issuer");
    var noAudiences = issuer("https://a/");
    noAudiences.remove("audiences");
    var emptyAudiences = issuer("https://a/");
    emptyAudiences.put("audiences", List.of());
    var twoKeySources = issuer("https://a/");
    twoKeySources.put("keys", List.of(KEY));
    twoKeySources.put("jwksUri", "https://a/jwks");
    var emptyKeys = issuer("https://a/");
    emptyKeys.put("keys", List.of());
    var noneAlgorithm = issuer("https://a/");
    noneAlgorithm.put("algorithms", List.of("none"));
    var unknownIssuerKey = issuer("https://a/");
    unknownIssuerKey.put("audience", List.of("x"));
    var negativeIssuerLeeway = issuer("https://a/");
    negativeIssuerLeeway.put("leeway", -1);
    var relativeJwksUri = issuer("https://a/");
    relativeJwksUri.put("jwksUri", "/jwks");

    return List.of(
        Arguments.of(Map.of(), "auth.issuers must be a non-empty array"),
        Arguments.of(Map.of("issuers", List.of()), "auth.issuers must be a non-empty array"),
        Arguments.of(
            Map.of("issuers", List.of("https://a/")), "auth.issuers[0] must be a JSON object"),
        Arguments.of(Map.of("issuers", List.of(noIssuer)), "auth.issuers[0].issuer is required"),
        Arguments.of(
            Map.of("issuers", List.of(noAudiences)), "auth.issuers[0].audiences is required"),
        Arguments.of(
            Map.of("issuers", List.of(emptyAudiences)),
            "auth.issuers[0].audiences must be a non-empty array of non-blank strings"),
        Arguments.of(
            Map.of("issuers", List.of(issuer("https://a/"), twoKeySources)),
            "auth.issuers[1] must set at most one of 'keys' and 'jwksUri'"),
        Arguments.of(
            Map.of("issuers", List.of(emptyKeys)),
            "auth.issuers[0].keys must be a non-empty array of JWK objects"),
        Arguments.of(
            Map.of("issuers", List.of(issuer("https://a/"), issuer("https://a/"))),
            "auth.issuers lists issuer 'https://a/' more than once"),
        Arguments.of(
            Map.of("issuers", List.of(unknownIssuerKey)),
            "auth.issuers[0] has unknown key(s) [audience], supported keys are [algorithms,"
                + " audiences, issuer, jwksUri, keys, leeway]"),
        Arguments.of(
            Map.of("issuers", List.of(issuer("https://a/")), "issuer", "https://a/"),
            "auth has unknown key(s) [issuer], supported keys are [algorithms, issuers, leeway,"
                + " resource, scopesSupported]"),
        Arguments.of(
            Map.of("issuers", List.of(issuer("https://a/")), "leeway", -5),
            "auth.leeway must not be negative"),
        Arguments.of(
            Map.of("issuers", List.of(negativeIssuerLeeway)),
            "auth.issuers[0].leeway must not be negative"),
        Arguments.of(
            Map.of("issuers", List.of(issuer("https://a/")), "leeway", "60"),
            "auth.leeway must be an integer number of seconds"),
        Arguments.of(
            Map.of("issuers", List.of(noneAlgorithm)),
            "auth.issuers[0].algorithms has unsupported value(s) [none], supported are [ES256,"
                + " ES384, ES512, EdDSA, HS256, HS384, HS512, PS256, PS384, PS512, RS256, RS384,"
                + " RS512]"),
        Arguments.of(
            Map.of("issuers", List.of(issuer("https://a/")), "algorithms", List.of()),
            "auth.algorithms must be a non-empty array of non-blank strings"),
        Arguments.of(
            Map.of("issuers", List.of(issuer("my-issuer"))),
            "auth.issuers[0].issuer must be an absolute http(s) URL to discover its keys, or set"
                + " 'keys' or 'jwksUri'"),
        Arguments.of(
            Map.of("issuers", List.of(relativeJwksUri)),
            "auth.issuers[0].jwksUri must be an absolute http(s) URL"));
  }

  @ParameterizedTest
  @MethodSource("invalidConfigs")
  void givenInvalidAuthConfig_whenServerConfigLoaded_thenRejectedWithClearError(
      Map<String, Object> auth, String expectedMessage) {
    assertThatThrownBy(() -> ServerConfigUtil.fromConfigMap(Map.of("auth", auth)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(expectedMessage);
  }

  @Test
  void givenAuthWithDeprecatedJwtAuth_whenServerConfigLoaded_thenRejected() {
    Map<String, Object> config =
        Map.of(
            "auth",
            Map.of("issuers", List.of(issuer("https://a/"))),
            "jwtAuth",
            Map.of("pubSecKeys", List.of()));

    assertThatThrownBy(() -> ServerConfigUtil.fromConfigMap(config))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith(
            "'auth' cannot be combined with the deprecated 'jwtAuth' or 'oauthConfig'");
  }

  @Test
  void givenAuthWithDeprecatedOauthConfig_whenServerConfigLoaded_thenRejected() {
    Map<String, Object> config =
        Map.of(
            "auth",
            Map.of("issuers", List.of(issuer("https://a/"))),
            "oauthConfig",
            Map.of("oauth2Options", Map.of("site", "https://idp.example.com")));

    assertThatThrownBy(() -> ServerConfigUtil.fromConfigMap(config))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith(
            "'auth' cannot be combined with the deprecated 'jwtAuth' or 'oauthConfig'");
  }

  @Test
  void givenAuth_whenServerConfigLoaded_thenProtectedResourceMetadataDerivedFromIssuers() {
    var serverConfig =
        ServerConfigUtil.fromConfigMap(
            Map.of(
                "auth",
                Map.of(
                    "issuers",
                    List.of(issuer("https://a/"), issuer("https://b/")),
                    "resource",
                    "https://api.example.com/v1/mcp")));

    assertThat(serverConfig.getProtectedResourceMetadata())
        .contains(
            new ProtectedResourceMetadata(
                List.of("https://a/", "https://b/"),
                "https://api.example.com/v1/mcp",
                List.of("mcp:tools", "mcp:resources")));
  }

  @Test
  void givenAuthWithOnlyInlineKeyIssuers_whenServerConfigLoaded_thenNoProtectedResourceMetadata() {
    var inlineKeys = issuer("my-backend");
    inlineKeys.put("keys", List.of(KEY));

    var serverConfig =
        ServerConfigUtil.fromConfigMap(Map.of("auth", Map.of("issuers", List.of(inlineKeys))));

    assertThat(serverConfig.getAuthConfig().issuers()).hasSize(1);
    assertThat(serverConfig.getProtectedResourceMetadata()).isEmpty();
  }

  @Test
  void givenNoAuthConfig_whenServerConfigLoaded_thenNoProtectedResourceMetadata() {
    var serverConfig = ServerConfigUtil.fromConfigMap(Map.of());

    assertThat(serverConfig.getAuthConfig()).isNull();
    assertThat(serverConfig.getProtectedResourceMetadata()).isEmpty();
  }

  private static AuthConfig.Issuer issuerWithAlgorithms(List<String> algorithms) {
    return new AuthConfig.Issuer("https://a/", List.of("api"), null, null, 60, algorithms);
  }

  private static AuthConfig parse(Map<String, Object> auth) {
    return ServerConfigUtil.fromConfigMap(Map.of("auth", auth)).getAuthConfig();
  }

  private static Map<String, Object> issuer(String issuer) {
    var entry = new HashMap<String, Object>();
    entry.put("issuer", issuer);
    entry.put("audiences", List.of("https://api.example.com/"));
    return entry;
  }
}
