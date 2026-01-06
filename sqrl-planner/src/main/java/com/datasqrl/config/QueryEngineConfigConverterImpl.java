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

import static com.datasqrl.graphql.SqrlObjectMapper.MAPPER;

import com.datasqrl.engine.database.QueryEngine;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor(onConstructor_ = @Inject)
public class QueryEngineConfigConverterImpl implements QueryEngineConfigConverter {

  private final ExecutionEnginesHolder enginesHolder;
  private final PackageJson packageJson;

  public List<ObjectNode> convertConfigsToJson() {
    var convertedConfigs = new ArrayList<ObjectNode>();
    var enabledQueryEngines = enginesHolder.getEngines(QueryEngine.class).values();

    for (var engine : enabledQueryEngines) {
      var queryEngine = (QueryEngine) engine;
      var engineConf = packageJson.getEngines().getEngineConfig(queryEngine.getName()).get();

      if (engineConf instanceof EngineConfigImpl impl) {
        var engineConfigMap = impl.sqrlConfig.toMap();

        var rootNode = MAPPER.createObjectNode();
        var configNode = MAPPER.valueToTree(engineConfigMap);
        rootNode.set(queryEngine.serverConfigName(), configNode);

        convertedConfigs.add(rootNode);
      }
    }

    return List.copyOf(convertedConfigs);
  }
}
