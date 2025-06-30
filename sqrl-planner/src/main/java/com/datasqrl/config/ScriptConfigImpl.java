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

import java.util.List;
import java.util.Optional;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class ScriptConfigImpl implements PackageJson.ScriptConfig {

  SqrlConfig sqrlConfig;

  public static final String MAIN_KEY = "main";
  public static final String GRAPHQL_KEY = "graphql";
  public static final String OPERATIONS_KEY = "operations";
  public static final String GRAPHQL_NORMALIZED_FILE_NAME = "schema.graphqls";

  @Override
  public Optional<String> getMainScript() {
    return sqrlConfig.asString(MAIN_KEY).getOptional();
  }

  @Override
  public Optional<String> getGraphql() {
    return sqrlConfig.asString(GRAPHQL_KEY).getOptional();
  }

  @Override
  public List<String> getOperations() {
    return sqrlConfig.asList(OPERATIONS_KEY, String.class).get();
  }

  @Override
  public void setMainScript(String script) {
    sqrlConfig.setProperty(MAIN_KEY, script);
  }

  @Override
  public void setGraphql(String graphql) {
    sqrlConfig.setProperty(GRAPHQL_KEY, graphql);
  }
}
