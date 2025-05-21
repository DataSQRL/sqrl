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
package com.datasqrl.engine.stream.flink;

import static com.datasqrl.engine.EngineFeature.STANDARD_STREAM;

import com.datasqrl.config.EngineType;
import com.datasqrl.config.ExecutionMode;
import com.datasqrl.config.PackageJson;
import com.datasqrl.config.PackageJson.EmptyEngineConfig;
import com.datasqrl.config.PackageJson.EngineConfig;
import com.datasqrl.engine.EngineFeature;
import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.engine.stream.StreamEngine;
import com.google.inject.Inject;
import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FlinkStreamEngine extends ExecutionEngine.Base implements StreamEngine {

  public static final String MODE_KEY = "mode";

  public static final EnumSet<EngineFeature> FLINK_CAPABILITIES = STANDARD_STREAM;

  @Getter private final EngineConfig engineConfig;

  @Inject
  public FlinkStreamEngine(PackageJson json) {
    super(FlinkEngineFactory.ENGINE_NAME, EngineType.STREAMS, FLINK_CAPABILITIES);
    this.engineConfig =
        json.getEngines()
            .getEngineConfig(FlinkEngineFactory.ENGINE_NAME)
            .orElseGet(() -> new EmptyEngineConfig(FlinkEngineFactory.ENGINE_NAME));
  }

  @Override
  public void close() throws IOException {}

  @Override
  public ExecutionMode getExecutionMode() {
    return ExecutionMode.valueOf(
        engineConfig
            .getSetting(MODE_KEY, Optional.of(ExecutionMode.STREAMING.name()))
            .toUpperCase(Locale.ENGLISH));
  }

  public Map<String, String> getBaseConfiguration() {
    Map<String, String> configMap = new HashMap<>();
    engineConfig.getConfig().forEach((key, value) -> configMap.put(key, String.valueOf(value)));
    return configMap;
  }
}
