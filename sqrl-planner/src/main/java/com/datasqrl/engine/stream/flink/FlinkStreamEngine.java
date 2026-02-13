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
import static org.apache.flink.configuration.CheckpointingOptions.CHECKPOINTING_INTERVAL;
import static org.apache.flink.configuration.CheckpointingOptions.MIN_PAUSE_BETWEEN_CHECKPOINTS;
import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_SOURCE_IDLE_TIMEOUT;

import com.datasqrl.config.EngineType;
import com.datasqrl.config.PackageJson;
import com.datasqrl.config.PackageJson.EngineConfig;
import com.datasqrl.engine.EngineFeature;
import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.engine.stream.StreamEngine;
import com.datasqrl.error.ErrorCollector;
import com.google.inject.Inject;
import java.io.IOException;
import java.time.Duration;
import java.util.EnumSet;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;

@Slf4j
public class FlinkStreamEngine extends ExecutionEngine.Base implements StreamEngine {

  public static final EnumSet<EngineFeature> FLINK_CAPABILITIES = STANDARD_STREAM;

  @Getter private final EngineConfig engineConfig;

  @Inject
  public FlinkStreamEngine(PackageJson json, ErrorCollector errors) {
    super(FlinkEngineFactory.ENGINE_NAME, EngineType.PROCESS, FLINK_CAPABILITIES);
    this.engineConfig = json.getEngines().getEngineConfigOrEmpty(FlinkEngineFactory.ENGINE_NAME);
    FlinkConfigValidator.validate(engineConfig.getConfig(), errors);
  }

  @Override
  public void close() throws IOException {}

  @Override
  public RuntimeExecutionMode getExecutionMode() {
    return RuntimeExecutionMode.valueOf(
        engineConfig.getConfig().get(ExecutionOptions.RUNTIME_MODE.key()).toString().toUpperCase());
  }

  public Configuration getBaseConfiguration() {
    var conf = new Configuration();
    engineConfig.getConfig().forEach((key, value) -> conf.setString(key, String.valueOf(value)));

    return conf;
  }

  public Configuration getStreamingSpecificConfig() {
    var conf = new Configuration();
    conf.set(TABLE_EXEC_SOURCE_IDLE_TIMEOUT, sec(1));
    conf.set(CHECKPOINTING_INTERVAL, sec(30));
    conf.set(MIN_PAUSE_BETWEEN_CHECKPOINTS, sec(20));

    return conf;
  }

  public Configuration getTemporalJoinConfig() {
    var conf = new Configuration();
    conf.set(TABLE_EXEC_SOURCE_IDLE_TIMEOUT, sec(10));

    return conf;
  }

  private Duration sec(int amount) {
    return Duration.ofSeconds(amount);
  }
}
