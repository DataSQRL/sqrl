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

import com.datasqrl.server.operation.ApiProtocol;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class CompilerApiConfigConverter implements ServerConfigConverter {

  private final PackageJson packageJson;

  @Override
  public void convert(ObjectNode serverConfig) {
    var apiConfig = packageJson.getCompilerConfig().getApiConfig();
    var protocols = apiConfig.getProtocols();
    // Make sure prop names matching with ServerConfig
    serverConfig.put("publicGraphQLEndpointEnabled", protocols.contains(ApiProtocol.GRAPHQL));
    serverConfig.put("onlyConfiguredGraphQLOperations", apiConfig.isOperationsOnly());
  }
}
