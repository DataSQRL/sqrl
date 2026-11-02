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

import java.util.Map;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class TestScriptConfig {

  public static PackageJson.ScriptConfig withIncludes(Map<String, Boolean> standaloneByNamespace) {
    var config = SqrlConfig.createCurrentVersion();
    var includes = config.getSubConfig(ScriptConfigImpl.INCLUDE_KEY);
    standaloneByNamespace.forEach(
        (namespace, standalone) -> {
          var include = includes.getSubConfig(namespace);
          include.setProperty("package", "../" + namespace + "/package.json");
          include.setProperty("standalone", standalone);
        });

    return new ScriptConfigImpl(config);
  }
}
