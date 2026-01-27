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
package com.datasqrl.graphql.config;

import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Spring Boot configuration properties for the SQRL GraphQL server. Replaces the Vert.x-based
 * ServerConfig.
 */
@Data
@Component
@ConfigurationProperties(prefix = "sqrl.server")
public class ServerConfigProperties {

  private ExecutionMode executionMode = ExecutionMode.ASYNC;
  private ServletConfig servletConfig = new ServletConfig();
  private PostgresConfig postgres = new PostgresConfig();
  private KafkaMutationConfig kafkaMutation;
  private KafkaSubscriptionConfig kafkaSubscription;
  private DuckDbConfig duckDb;
  private SnowflakeConfig snowflake;
  private CorsConfig cors = new CorsConfig();
  private SwaggerConfig swagger = new SwaggerConfig();
  private GraphiQLConfig graphiql;
  private JwtConfig jwt;
  private OAuthConfig oauth;

  public enum ExecutionMode {
    ASYNC,
    SYNC
  }

  @Data
  public static class ServletConfig {
    private String graphqlEndpoint = "/graphql";
    private String graphiqlEndpoint = "/graphiql/*";
    private String restEndpoint = "/rest";
    private String mcpEndpoint = "/mcp";

    public String getGraphQLEndpoint(String modelVersion) {
      return modelVersion.isEmpty() ? graphqlEndpoint : "/" + modelVersion + graphqlEndpoint;
    }

    public String getGraphiQLEndpoint(String modelVersion) {
      return modelVersion.isEmpty() ? graphiqlEndpoint : "/" + modelVersion + graphiqlEndpoint;
    }

    public String getRestEndpoint(String modelVersion) {
      return modelVersion.isEmpty() ? restEndpoint : "/" + modelVersion + restEndpoint;
    }

    public String getMcpEndpoint(String modelVersion) {
      return modelVersion.isEmpty() ? mcpEndpoint : "/" + modelVersion + mcpEndpoint;
    }
  }

  @Data
  public static class PostgresConfig {
    private String host = "localhost";
    private int port = 5432;
    private String database = "datasqrl";
    private String user = "postgres";
    private String password = "";
    private int poolSize = 10;
    private int maxIdleTime = 30000;
  }

  @Data
  public static class KafkaMutationConfig {
    private Map<String, String> properties;
    private boolean transactional = false;

    public Map<String, String> asMap(boolean transactional) {
      return properties;
    }
  }

  @Data
  public static class KafkaSubscriptionConfig {
    private Map<String, String> properties;

    public Map<String, String> asMap() {
      return properties;
    }
  }

  @Data
  public static class DuckDbConfig {
    private String path = ":memory:";
    private int poolSize = 5;
    private List<String> extensions;
  }

  @Data
  public static class SnowflakeConfig {
    private String url;
    private String user;
    private String password;
    private String database;
    private String schema;
    private String warehouse;
    private Map<String, String> properties;
  }

  @Data
  public static class CorsConfig {
    private String allowedOrigin;
    private List<String> allowedOrigins;
    private Set<String> allowedMethods = Set.of("GET", "POST", "OPTIONS");
    private Set<String> allowedHeaders = Set.of("*");
    private Set<String> exposedHeaders = Set.of();
    private boolean allowCredentials = false;
    private int maxAgeSeconds = 3600;
    private boolean allowPrivateNetwork = false;
  }

  @Data
  public static class SwaggerConfig {
    private boolean enabled = false;
    private String title = "SQRL GraphQL API";
    private String description = "Auto-generated REST API from GraphQL schema";
    private String version = "1.0.0";

    public String getEndpoint(String modelVersion) {
      return modelVersion.isEmpty() ? "/swagger.json" : "/" + modelVersion + "/swagger.json";
    }

    public String getUiEndpoint(String modelVersion) {
      return modelVersion.isEmpty() ? "/swagger-ui" : "/" + modelVersion + "/swagger-ui";
    }
  }

  @Data
  public static class GraphiQLConfig {
    private boolean enabled = true;
    private String graphqlEndpoint = "/graphql";
  }

  @Data
  public static class JwtConfig {
    private String publicKey;
    private String publicKeyPath;
    private String jwksUri;
    private String issuer;
    private List<String> audience;
  }

  @Data
  public static class OAuthConfig {
    private String site;
    private String clientId;
    private String clientSecret;
    private String introspectionPath;
    private String jwksPath;
  }
}
