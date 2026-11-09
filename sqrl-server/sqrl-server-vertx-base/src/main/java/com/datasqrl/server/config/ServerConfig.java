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

import com.datasqrl.env.EnvVariableNames;
import com.datasqrl.env.GlobalEnvironmentStore;
import com.datasqrl.util.JsonUtils;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.jwt.JWTAuthOptions;
import io.vertx.ext.web.handler.graphql.GraphQLHandlerOptions;
import io.vertx.ext.web.handler.graphql.GraphiQLHandlerOptions;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.sqlclient.PoolOptions;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Getter
@Setter
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class ServerConfig {

  @Getter(AccessLevel.NONE)
  private final ObjectMapper MAPPER = JsonUtils.getMapperWithEnvVarResolver(null);

  private ServletConfig servletConfig = new ServletConfig();
  private GraphQLHandlerOptions graphQLHandlerOptions = new GraphQLHandlerOptions();
  private GraphiQLHandlerOptions graphiQLHandlerOptions;
  private boolean publicGraphQLEndpointEnabled = true;
  private boolean onlyConfiguredGraphQLOperations;
  private long gracefulShutdownTimeoutSeconds = 15;

  @JsonSerialize(using = HttpServerOptionsSerializer.class)
  private HttpServerOptions httpServerOptions = new HttpServerOptions();

  private PgConnectOptions pgConnectOptions = new PgConnectOptions();
  private PoolOptions poolOptions = new PoolOptions();
  private CorsHandlerOptions corsHandlerOptions = new CorsHandlerOptions();
  private OpenApiConfig openApiConfig = new OpenApiConfig();
  private GraphQLTailSampleTracingConfig graphQLTailSampleTracingConfig =
      new GraphQLTailSampleTracingConfig();
  private GraphQLParserConfig graphQLParserConfig = new GraphQLParserConfig();
  private HeavyQueryLogConfig heavyQueryLogConfig = new HeavyQueryLogConfig();
  private Map<String, Object> auth;
  private Map<String, Object> jwtAuth;
  private OAuthConfig oauthConfig;
  private boolean logForwardedClientAddress = true;

  private KafkaConfig.KafkaMutationConfig kafkaMutationConfig;
  private KafkaConfig.KafkaSubscriptionConfig kafkaSubscriptionConfig;
  private JdbcConfig.DuckDbConfig duckDbConfig;
  private JdbcConfig snowflakeConfig;

  /**
   * Validates and applies post-deserialization logic to this ServerConfig instance. This method
   * should be called after any Jackson deserialization.
   *
   * @return this ServerConfig instance for method chaining
   */
  public ServerConfig validated() {
    // Apply PostgreSQL port from environment variable if set
    var pgPort = GlobalEnvironmentStore.get(EnvVariableNames.POSTGRES_PORT);
    try {
      if (pgPort != null && !pgPort.isEmpty()) {
        pgConnectOptions.setPort(Integer.parseInt(pgPort));
      }
    } catch (NumberFormatException ignored) {
      // Ignore invalid port numbers
    }

    // Validate Kafka configs if present
    if (kafkaMutationConfig != null) {
      kafkaMutationConfig.validateConfig();
    }
    if (kafkaSubscriptionConfig != null) {
      kafkaSubscriptionConfig.validateConfig();
    }

    // Validate DuckDB config if present
    if (duckDbConfig != null) {
      duckDbConfig.validateConfig();
    }

    validateAuth();

    return this;
  }

  private void validateAuth() {
    if (auth != null) {
      checkArgument(
          jwtAuth == null && oauthConfig == null,
          "'auth' cannot be combined with the deprecated 'jwtAuth' or 'oauthConfig'; "
              + "set them to null in the configuration layer that introduces 'auth'");
      getAuthConfig();
    }

    if (jwtAuth != null) {
      log.warn("'jwtAuth' is deprecated and will be removed, configure 'auth' instead");
      checkArgument(
          hasVerificationKey(new JsonObject(jwtAuth)),
          "'jwtAuth' has no key that can verify signatures, so the server would accept unsigned"
              + " tokens. Set 'pubSecKeys', 'jwks' or 'keyStore', or migrate to 'auth'");
    }

    if (oauthConfig != null) {
      log.warn("'oauthConfig' is deprecated and will be removed, configure 'auth' instead");
    }
  }

  private static boolean hasVerificationKey(JsonObject jwtAuth) {
    return jwtAuth.getValue("keyStore") != null
        || jwtAuth.getValue("pubSecKeys") instanceof JsonArray pemKeys && !pemKeys.isEmpty()
        || jwtAuth.getValue("jwks") instanceof JsonArray jwks
            && jwks.stream()
                .anyMatch(
                    jwk -> jwk instanceof JsonObject key && AuthConfig.canVerifySignatures(key));
  }

  /**
   * Parses the {@code auth} block with environment variables resolved, or returns null if not set.
   * It parses on every call, so call it at startup, not per request.
   */
  @JsonIgnore
  public AuthConfig getAuthConfig() {
    return auth == null ? null : AuthConfig.parse(resolveEnvVars(auth));
  }

  /**
   * Returns the RFC 9728 protected resource metadata, or empty if no OAuth authorization server is
   * configured. With {@code auth}, issuers with inline keys are not advertised.
   */
  @JsonIgnore
  public Optional<ProtectedResourceMetadata> getProtectedResourceMetadata() {
    var authConfig = getAuthConfig();
    if (authConfig != null) {
      var authorizationServers = authConfig.authorizationServers();
      return authorizationServers.isEmpty()
          ? Optional.empty()
          : Optional.of(
              new ProtectedResourceMetadata(
                  authorizationServers, authConfig.resource(), authConfig.scopesSupported()));
    }

    if (hasOAuthProvider()) {
      var scopes = oauthConfig.getScopesSupported();
      return Optional.of(
          new ProtectedResourceMetadata(
              List.of(oauthConfig.getEffectiveAuthorizationServer()),
              oauthConfig.getResource(),
              scopes != null ? scopes : AuthConfig.DEFAULT_SCOPES));
    }

    return Optional.empty();
  }

  /**
   * Returns the JWT authentication options with environment variables resolved. This custom getter
   * exists because {@code jwtAuth} is stored as a raw JSON string to defer environment variable
   * resolution before runtime, causing init failure in {@link JWTAuthOptions}.
   */
  @JsonIgnore
  public JWTAuthOptions getJwtAuthOptions() {
    return jwtAuth == null ? null : new JWTAuthOptions(resolveEnvVars(jwtAuth));
  }

  /**
   * Converts a raw config block to JSON with environment variables resolved, for use in memory
   * only. Do not store the result on this object: the raw blocks are what gets serialized, so the
   * compiled {@code vertx-config.json} keeps its {@code ${VAR}} placeholders and resolved values
   * never leak into a deployment artifact.
   */
  @SneakyThrows
  private JsonObject resolveEnvVars(Map<String, Object> block) {
    return MAPPER.readValue(MAPPER.writeValueAsString(block), JsonObject.class);
  }

  /**
   * Returns true when the server creates at least one auth provider: from the {@code auth} block,
   * which {@link AuthConfig#parse} accepts only with an issuer, from {@code jwtAuth}, or from
   * {@code oauthConfig} with a site URL.
   */
  public boolean hasAuthProvider() {
    return auth != null || hasJwtAuthProvider() || hasOAuthProvider();
  }

  /** Returns true when {@code jwtAuth} is set, so the server creates a JWT auth provider. */
  public boolean hasJwtAuthProvider() {
    return jwtAuth != null;
  }

  /**
   * Returns true when {@code oauthConfig} has a site URL, so the server creates an OAuth auth
   * provider.
   */
  public boolean hasOAuthProvider() {
    return oauthConfig != null && oauthConfig.getSite() != null;
  }

  ////////////////////////////////////////////////////////////////////////////////
  // Custom JSON setters for Jackson deserialization of Vert.x classes
  ////////////////////////////////////////////////////////////////////////////////

  @JsonSetter("graphQLHandlerOptions")
  public void setGraphQLHandlerOptionsFromJson(Map<String, Object> options) {
    this.graphQLHandlerOptions = new GraphQLHandlerOptions(getJsonObjectOrEmpty(options));
  }

  @JsonSetter("httpServerOptions")
  public void setHttpServerOptionsFromJson(Map<String, Object> options) {
    this.httpServerOptions = new HttpServerOptions(getJsonObjectOrEmpty(options));
  }

  @JsonSetter("pgConnectOptions")
  public void setPgConnectOptionsFromJson(Map<String, Object> options) {
    this.pgConnectOptions = new PgConnectOptions(getJsonObjectOrEmpty(options));
  }

  @JsonSetter("poolOptions")
  public void setPoolOptionsFromJson(Map<String, Object> options) {
    this.poolOptions = new PoolOptions(getJsonObjectOrEmpty(options));
  }

  @JsonSetter("graphiQLHandlerOptions")
  public void setGraphiQLHandlerOptionsFromJson(Map<String, Object> options) {
    this.graphiQLHandlerOptions =
        options == null ? null : new GraphiQLHandlerOptions(new JsonObject(options));
  }

  ////////////////////////////////////////////////////////////////////////////////
  // Custom JSON setters for Jackson deserialization of our own POJO classes
  ////////////////////////////////////////////////////////////////////////////////

  @JsonSetter("servletConfig")
  public void setServletConfigFromJson(Map<String, Object> options) {
    this.servletConfig =
        options == null ? new ServletConfig() : MAPPER.convertValue(options, ServletConfig.class);
  }

  @JsonSetter("corsHandlerOptions")
  public void setCorsHandlerOptionsFromJson(Map<String, Object> options) {
    this.corsHandlerOptions =
        options == null
            ? new CorsHandlerOptions()
            : MAPPER.convertValue(options, CorsHandlerOptions.class);
  }

  @JsonSetter("openApiConfig")
  public void setOpenApiConfigFromJson(Map<String, Object> options) {
    this.openApiConfig =
        options == null ? new OpenApiConfig() : MAPPER.convertValue(options, OpenApiConfig.class);
  }

  @JsonSetter("graphQLTailSampleTracingConfig")
  public void setGraphQLTailSampleTracingConfigFromJson(Map<String, Object> options) {
    this.graphQLTailSampleTracingConfig =
        options == null
            ? new GraphQLTailSampleTracingConfig()
            : MAPPER.convertValue(options, GraphQLTailSampleTracingConfig.class);
  }

  @JsonSetter("graphQLParserConfig")
  public void setGraphQLParserConfigFromJson(Map<String, Object> options) {
    this.graphQLParserConfig =
        options == null
            ? new GraphQLParserConfig()
            : MAPPER.convertValue(options, GraphQLParserConfig.class);
  }

  @JsonSetter("heavyQueryLogConfig")
  public void setHeavyQueryLogConfigFromJson(Map<String, Object> options) {
    this.heavyQueryLogConfig =
        options == null
            ? new HeavyQueryLogConfig()
            : MAPPER.convertValue(options, HeavyQueryLogConfig.class);
  }

  private JsonObject getJsonObjectOrEmpty(Map<String, Object> options) {
    return options == null ? new JsonObject() : new JsonObject(options);
  }

  private static class HttpServerOptionsSerializer extends JsonSerializer<HttpServerOptions> {

    @Override
    public void serialize(
        HttpServerOptions value, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      gen.writeObject(value.toJson().getMap());
    }
  }
}
