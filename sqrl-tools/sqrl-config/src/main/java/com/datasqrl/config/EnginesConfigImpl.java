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

import com.datasqrl.config.PackageJson.EngineConfig;
import java.util.Optional;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class EnginesConfigImpl implements PackageJson.EnginesConfig {
  private SqrlConfig sqrlConfig;

  public int getVersion() {
    return sqrlConfig.getVersion();
  }

  @Override
  public Optional<EngineConfig> getEngineConfig(String engineId) {
    if (!sqrlConfig.hasSubConfig(engineId)) {
      return Optional.empty();
    }
    var subConfig = sqrlConfig.getSubConfig(engineId);
    return Optional.of(new EngineConfigImpl(subConfig));
  }

  @Override
  public EngineConfig getEngineConfigOrErr(String engineId) {
    sqrlConfig.validateSubConfig(engineId);
    return new EngineConfigImpl(sqrlConfig.getSubConfig(engineId));
  }
}
