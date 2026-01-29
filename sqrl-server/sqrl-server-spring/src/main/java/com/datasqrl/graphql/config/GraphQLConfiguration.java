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

import com.datasqrl.graphql.SpringContext;
import com.datasqrl.graphql.jdbc.DatabaseType;
import com.datasqrl.graphql.jdbc.SpringJdbcClient;
import com.datasqrl.graphql.kafka.SpringMutationConfiguration;
import com.datasqrl.graphql.kafka.SpringSubscriptionConfiguration;
import com.datasqrl.graphql.server.CustomScalars;
import com.datasqrl.graphql.server.FunctionExecutor;
import com.datasqrl.graphql.server.GraphQLEngineBuilder;
import com.datasqrl.graphql.server.MetadataReader;
import com.datasqrl.graphql.server.MetadataType;
import com.datasqrl.graphql.server.ModelContainer;
import com.datasqrl.graphql.server.RootGraphqlModel;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.symbaloo.graphqlmicrometer.MicrometerInstrumentation;
import graphql.GraphQL;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * GraphQL configuration for Spring Boot. Creates and configures the GraphQL engine with all
 * necessary dependencies.
 */
@Slf4j
@Configuration
@RequiredArgsConstructor
public class GraphQLConfiguration {

  private final ServerConfigProperties config;
  private final MeterRegistry meterRegistry;

  @Bean
  @SneakyThrows
  public Map<String, RootGraphqlModel> rootGraphqlModels(ObjectMapper objectMapper) {
    var modelFile = new File("vertx.json");
    if (!modelFile.exists()) {
      log.warn("vertx.json not found, GraphQL models will be empty");
      return Map.of();
    }

    return objectMapper.readValue(modelFile, ModelContainer.class).models;
  }

  @Bean
  public SpringJdbcClient springJdbcClient(Map<DatabaseType, Object> databaseClients) {
    var asyncMode = config.getExecutionMode() == ServerConfigProperties.ExecutionMode.ASYNC;
    return new SpringJdbcClient(databaseClients, asyncMode);
  }

  @Bean
  public SpringMutationConfiguration springMutationConfiguration() {
    return new SpringMutationConfiguration(config);
  }

  @Bean
  public SpringSubscriptionConfiguration springSubscriptionConfiguration() {
    return new SpringSubscriptionConfiguration(config);
  }

  @Bean
  public Map<String, GraphQL> graphQLEngines(
      Map<String, RootGraphqlModel> models,
      SpringJdbcClient springJdbcClient,
      SpringMutationConfiguration mutationConfiguration,
      SpringSubscriptionConfiguration subscriptionConfiguration) {

    Map<String, GraphQL> engines = new HashMap<>();

    for (var entry : models.entrySet()) {
      var modelVersion = entry.getKey();
      var model = entry.getValue();

      var engine =
          createGraphQL(
              model,
              springJdbcClient,
              mutationConfiguration,
              subscriptionConfiguration,
              createMetadataReaders(),
              createFunctionExecutor());

      engines.put(modelVersion, engine);
      log.info("Created GraphQL engine for model version: {}", modelVersion);
    }

    return engines;
  }

  private GraphQL createGraphQL(
      RootGraphqlModel model,
      SpringJdbcClient jdbcClient,
      SpringMutationConfiguration mutationConfiguration,
      SpringSubscriptionConfiguration subscriptionConfiguration,
      Map<MetadataType, MetadataReader> metadataReaders,
      FunctionExecutor functionExecutor) {

    try {
      var context = new SpringContext(jdbcClient, metadataReaders, functionExecutor);

      var graphQL =
          model.accept(
              new GraphQLEngineBuilder.Builder()
                  .withMutationConfiguration(mutationConfiguration)
                  .withSubscriptionConfiguration(subscriptionConfiguration)
                  .withExtendedScalarTypes(CustomScalars.getExtendedScalars())
                  .build(),
              context);

      if (meterRegistry != null) {
        graphQL.instrumentation(new MicrometerInstrumentation(meterRegistry));
      }

      return graphQL.build();
    } catch (Exception e) {
      log.error("Unable to create GraphQL engine", e);
      throw e;
    }
  }

  private Map<MetadataType, MetadataReader> createMetadataReaders() {
    var readers = ImmutableMap.<MetadataType, MetadataReader>builder();

    if (config.getJwt() != null || config.getOauth() != null) {
      log.debug("Configuring authentication metadata reader");
      readers.put(MetadataType.AUTH, new SpringAuthMetadataReader());
    }

    return readers.build();
  }

  private FunctionExecutor createFunctionExecutor() {
    return (functionId, arguments) -> {
      throw new UnsupportedOperationException(
          "Function execution not yet implemented in Spring version");
    };
  }

  private static class SpringAuthMetadataReader implements MetadataReader {
    @Override
    public Object read(graphql.schema.DataFetchingEnvironment env, String name, boolean required) {
      var graphqlContext = env.getGraphQlContext();
      var claims = graphqlContext.get("claims");
      if (claims instanceof Map<?, ?> claimsMap) {
        var value = claimsMap.get(name);
        if (value == null && required) {
          throw new RuntimeException("Required claim not found: " + name);
        }
        return value;
      }
      if (required) {
        throw new RuntimeException("Authentication claims not available");
      }
      return null;
    }
  }
}
