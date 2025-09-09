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

import static com.datasqrl.graphql.SqrlObjectMapper.MAPPER;
import static org.apache.commons.lang3.ObjectUtils.isEmpty;

import com.datasqrl.util.JsonMergeUtils;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.vertx.ext.web.handler.graphql.GraphiQLHandlerOptions;
import jakarta.annotation.Nullable;
import java.util.Map;
import lombok.SneakyThrows;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

@UtilityClass
@Slf4j
public class ServerConfigUtil {

  /**
   * Merges server configuration with override values using Jackson.
   *
   * @param serverConfig base server configuration
   * @param configOverrides map of configuration overrides
   * @return merged and validated server configuration
   */
  @SneakyThrows
  public static ServerConfig mergeConfigs(
      ServerConfig serverConfig, Map<String, Object> configOverrides) {
    if (isEmpty(configOverrides)) {
      return serverConfig;
    }
    var config = ((ObjectNode) MAPPER.valueToTree(serverConfig)).deepCopy();
    JsonMergeUtils.merge(config, MAPPER.valueToTree(configOverrides));
    return MAPPER.treeToValue(config, ServerConfig.class).validated();
  }

  /**
   * Creates a ServerConfig from a configuration map using Jackson deserialization.
   *
   * @param configMap map containing configuration values
   * @return deserialized and validated ServerConfig instance
   */
  @SneakyThrows
  public static ServerConfig fromConfigMap(Map<String, Object> configMap) {
    return MAPPER.convertValue(configMap, ServerConfig.class).validated();
  }

  /**
   * Creates a copy of GraphiQL handler options with versioned URIs.
   *
   * @param version the version prefix to prepend to endpoints
   * @param graphiQLHandlerOptions the original options to copy and version
   * @return a new GraphiQL handler options instance with versioned endpoints, or null if input is
   *     null
   */
  @Nullable
  public static GraphiQLHandlerOptions createVersionedGraphiQLHandlerOptions(
      String version, @Nullable GraphiQLHandlerOptions graphiQLHandlerOptions) {
    if (graphiQLHandlerOptions == null) {
      return null;
    }

    var versionedOptions = new GraphiQLHandlerOptions(graphiQLHandlerOptions);
    var versionedGraphQLUri = getVersionedEndpoint(version, versionedOptions.getGraphQLUri());
    versionedOptions.setGraphQLUri(versionedGraphQLUri);

    var versionedGraphQLWSUri = getVersionedEndpoint(version, versionedOptions.getGraphQLWSUri());
    versionedOptions.setGraphWSQLUri(versionedGraphQLWSUri);

    return versionedOptions;
  }

  /**
   * Prepends a version prefix to an endpoint path. Assumes {@code endpoint} start with '/'.
   *
   * @param version the version prefix to prepend
   * @param endpoint the endpoint path to version
   * @return the versioned endpoint as "/{version}{endpoint}", or null if endpoint is null
   */
  @Nullable
  public static String getVersionedEndpoint(String version, @Nullable String endpoint) {
    return endpoint == null ? null : '/' + version + endpoint;
  }
}
