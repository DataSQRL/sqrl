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

import static com.datasqrl.graphql.config.ServerConfigUtil.getVersionedEndpoint;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class ServletConfig {

  private String graphiQLEndpoint = "/graphiql*";
  private String graphQLEndpoint = "/graphql";
  private String restEndpoint = "/rest";
  private String mcpEndpoint = "/mcp";
  private boolean usePgPool = true;

  public String getGraphiQLEndpoint(String version) {
    return getVersionedEndpoint(version, graphiQLEndpoint);
  }

  public String getGraphQLEndpoint(String version) {
    return getVersionedEndpoint(version, graphQLEndpoint);
  }

  public String getRestEndpoint(String version) {
    return getVersionedEndpoint(version, restEndpoint);
  }

  public String getMcpEndpoint(String version) {
    return getVersionedEndpoint(version, mcpEndpoint);
  }
}
