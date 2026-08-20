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

import static com.datasqrl.config.PackageJsonImpl.CONFIG_KEY;

import com.google.common.collect.Iterables;
import java.util.Map;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
public class EngineConfigImpl implements PackageJson.EngineConfig {

  @Getter SqrlConfig sqrlConfig;

  final String ENGINE_NAME_KEY = "type";

  @Override
  public String getEngineName() {
    return sqrlConfig.asString(ENGINE_NAME_KEY).get();
  }

  @Override
  public String getProperty(String key, Optional<String> defaultValue) {
    return getPropertyAs(key, String.class, defaultValue);
  }

  @Override
  public <T> T getPropertyAs(String key, Class<T> valueClass, Optional<T> defaultValue) {
    var result = sqrlConfig.as(key, valueClass);
    if (defaultValue.isPresent()) {
      result = result.withDefault(defaultValue.get());
    }

    return result.get();
  }

  @Override
  public Optional<String> getPropertyOptional(String key) {
    return getPropertyAsOptional(key, String.class);
  }

  @Override
  public <T> Optional<T> getPropertyAsOptional(String key, Class<T> valueClass) {
    var result = sqrlConfig.as(key, valueClass);

    return result.getOptional();
  }

  @Override
  public Map<String, Object> getConfig() {
    return sqrlConfig.getSubConfig(CONFIG_KEY).toMap();
  }

  @Override
  public boolean isEmpty() {
    return Iterables.isEmpty(sqrlConfig.getSubConfig(CONFIG_KEY).getKeys());
  }
}
