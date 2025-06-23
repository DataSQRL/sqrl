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
import com.datasqrl.graphql.server.operation.ApiProtocols;
import java.util.EnumSet;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@Getter
@Builder
@AllArgsConstructor
public class ApiConfigImpl implements CompilerApiConfig {

  /** The protocols that are being exposed by the server and generated from SQRL */
  @Default EnumSet<ApiProtocols> protocols = EnumSet.allOf(ApiProtocols.class);

  /** How to generate the endpoints */
  @Default Endpoints endpoints = Endpoints.FULL;

  /** This suffix it appended to all table names (before the uid) */
  @Default boolean addOperationsPrefix = true;

  /** The maximum depth of graph traversal when generating operations from schema */
  @Default int maxResultDepth = 3;

  public static ApiConfigImpl from(SqrlConfig config) {
    var builder = builder();

    List<ApiProtocols> protocols =
        config.asList("protocols", String.class).get().stream()
            .map(str -> ApiProtocols.valueOf(str.toUpperCase()))
            .toList();
    if (!protocols.isEmpty()) builder.protocols(EnumSet.copyOf(protocols));
    config
        .asString("endpoints")
        .getOptional()
        .map(str -> Endpoints.valueOf(str.toUpperCase()))
        .ifPresent(builder::endpoints);
    config.asBool("add-prefix").getOptional().ifPresent(builder::addOperationsPrefix);
    config.asInt("result-depth").getOptional().ifPresent(builder::maxResultDepth);
    return builder.build();
  }

  @Override
  public boolean isGraphQLProtocolOnly() {
    return protocols.contains(ApiProtocols.GRAPHQL) && protocols.size() == 1;
  }

  public boolean generateOperations() {
    return endpoints == Endpoints.FULL;
  }

  public enum Endpoints {
    OPS_ONLY,
    GRAPHQL,
    FULL;
  }
}
