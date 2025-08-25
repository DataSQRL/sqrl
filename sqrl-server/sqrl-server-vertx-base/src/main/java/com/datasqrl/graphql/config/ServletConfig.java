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

import io.vertx.core.json.JsonObject;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
public class ServletConfig {

  private String graphiQLEndpoint = "/graphiql*";
  private String graphQLEndpoint = "/graphql";
  private String restEndpoint = "/rest";
  private String mcpEndpoint = "/mcp";
  private boolean usePgPool = true;

  public ServletConfig(JsonObject json) {
    for (java.util.Map.Entry<String, Object> member : json) {
      switch (member.getKey()) {
        case "graphiQLEndpoint":
          if (member.getValue() instanceof String) {
            this.graphiQLEndpoint = (String) member.getValue();
          }
          break;
        case "graphQLEndpoint":
          if (member.getValue() instanceof String) {
            this.graphQLEndpoint = (String) member.getValue();
          }
          break;
        case "restEndpoint":
          if (member.getValue() instanceof String) {
            this.restEndpoint = (String) member.getValue();
          }
          break;
        case "mcpEndpoint":
          if (member.getValue() instanceof String) {
            this.mcpEndpoint = (String) member.getValue();
          }
          break;
        case "usePgPool":
          if (member.getValue() instanceof Boolean) {
            this.usePgPool = (Boolean) member.getValue();
          }
          break;
      }
    }
  }

  public String getGraphiQLEndpoint(String version) {
    return getVersioned(version, graphiQLEndpoint);
  }

  public String getGraphQLEndpoint(String version) {
    return getVersioned(version, graphQLEndpoint);
  }

  public String getRestEndpoint(String version) {
    return getVersioned(version, restEndpoint);
  }

  public String getMcpEndpoint(String version) {
    return getVersioned(version, mcpEndpoint);
  }

  private String getVersioned(String version, String endpoint) {
    return '/' + version + endpoint;
  }
}
