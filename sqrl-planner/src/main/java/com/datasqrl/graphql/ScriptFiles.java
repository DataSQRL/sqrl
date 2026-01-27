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
package com.datasqrl.graphql;

import com.datasqrl.config.PackageJson;
import com.datasqrl.config.PackageJson.ScriptApiConfig;
import com.datasqrl.config.PackageJson.ScriptConfig;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Optional;
import lombok.Getter;
import org.springframework.stereotype.Component;

@Component
@Getter
public class ScriptFiles {

  private final ScriptConfig config;
  private final Optional<String> graphql;
  private final List<String> operations;
  private final List<ScriptApiConfig> apiConfigs;

  @Inject
  public ScriptFiles(PackageJson rootConfig) {
    config = rootConfig.getScriptConfig();
    graphql = config.getGraphql();
    operations = config.getOperations();
    apiConfigs = config.getScriptApiConfigs();
  }
}
