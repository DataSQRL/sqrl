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
package com.datasqrl.config;

import com.datasqrl.config.PackageJson.CompilerApiConfig;
import com.datasqrl.server.operation.ApiProtocol;
import java.util.EnumSet;
import java.util.Locale;
import java.util.regex.Pattern;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class CompilerApiConfigImpl implements CompilerApiConfig {

  // RFC 3986 scheme followed by ':'
  private static final Pattern URI_SCHEME = Pattern.compile("^[A-Za-z][A-Za-z0-9+.-]*:");
  private static final String MCP_APPS_SCHEME = "ui:";

  private final SqrlConfig sqrlConfig;

  @Override
  public EnumSet<ApiProtocol> getProtocols() {
    return EnumSet.copyOf(sqrlConfig.asList("protocols", ApiProtocol.class).get());
  }

  @Override
  public boolean isGraphQLProtocolOnly() {
    var protocols = getProtocols();

    return protocols.contains(ApiProtocol.GRAPHQL) && protocols.size() == 1;
  }

  @Override
  public boolean generateOperations() {
    return sqrlConfig.as("endpoints", Endpoints.class).get() == Endpoints.FULL;
  }

  @Override
  public boolean isOperationsOnly() {
    return sqrlConfig.as("endpoints", Endpoints.class).get() == Endpoints.OPS_ONLY;
  }

  @Override
  public boolean isAddOperationsPrefix() {
    return sqrlConfig.asBool("add-prefix").get();
  }

  @Override
  public String getMcpResourceUriPrefix() {
    return sqrlConfig
        .asString("mcp-resource-uri-prefix")
        .validate(
            prefix -> URI_SCHEME.matcher(prefix).find(),
            "It must start with a URI scheme, such as 'sqrl://{version}/'.")
        .validate(
            prefix -> !prefix.toLowerCase(Locale.ROOT).startsWith(MCP_APPS_SCHEME),
            "The 'ui://' scheme is reserved for MCP Apps.")
        .get();
  }

  @Override
  public int getMaxResultDepth() {
    return sqrlConfig.asInt("max-result-depth").get();
  }

  @Override
  public int getDefaultLimit() {
    return sqrlConfig.asInt("default-limit").get();
  }

  @Override
  public boolean generatePaginatedResults() {
    return sqrlConfig.asBool("paginated-results").get();
  }

  public enum Endpoints {
    OPS_ONLY, // only support the pre-defined operations in the GraphQL API, do not support flexible
    // GraphQL queries
    GRAPHQL, // support flexible GraphQL API but only pre-defined operations for other protocols
    FULL; // support flexible GraphQL API and add generated operations from GraphQL schema to
    // pre-defined ones for other protocols
  }
}
