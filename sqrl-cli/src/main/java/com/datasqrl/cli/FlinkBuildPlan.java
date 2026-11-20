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
package com.datasqrl.cli;

import com.datasqrl.deployment.model.FlinkJobPlan;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.util.ConfigLoaderUtils;
import com.datasqrl.util.SqrlObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import org.apache.flink.configuration.PipelineOptions;

/**
 * The plan folder of one build and the Flink job in it.
 *
 * @param planDir the plan folder, for example {@code build/deploy/plan}
 * @param flinkJobPlan the compiled plan and {@code pipeline.max-parallelism} of the Flink job,
 *     empty when the folder has no {@code flink-compiled-plan.json}
 */
record FlinkBuildPlan(Path planDir, Optional<FlinkJobPlan> flinkJobPlan) {

  private static final String COMPILED_PLAN_FILE = "flink-compiled-plan.json";
  private static final String FLINK_CONFIG_FILE = "flink-config.yaml";

  /** Reads the plan folder. A Flink file that exists but cannot be read is a fatal error. */
  static FlinkBuildPlan read(Path planDir, ErrorCollector errors) {
    var flinkJobPlan =
        readCompiledPlan(planDir, errors)
            .map(
                compiledPlan ->
                    new FlinkJobPlan(compiledPlan, readMaxParallelism(planDir, errors)));
    return new FlinkBuildPlan(planDir, flinkJobPlan);
  }

  private static Optional<JsonNode> readCompiledPlan(Path planDir, ErrorCollector errors) {
    var compiledPlanFile = planDir.resolve(COMPILED_PLAN_FILE);
    if (!Files.isRegularFile(compiledPlanFile)) {
      return Optional.empty();
    }

    JsonNode compiledPlan;
    try {
      compiledPlan = SqrlObjectMapper.INSTANCE.readTree(compiledPlanFile.toFile());
    } catch (IOException exception) {
      throw cannotRead(compiledPlanFile, exception, errors);
    }
    // An empty file gives a missing node, so this also rejects an empty file.
    errors.checkFatal(
        compiledPlan.path("nodes").isArray(),
        "%s",
        "%s is not a compiled Flink plan: it has no \"nodes\" list".formatted(compiledPlanFile));
    return Optional.of(compiledPlan);
  }

  private static Optional<Integer> readMaxParallelism(Path planDir, ErrorCollector errors) {
    var flinkConfigFile = planDir.resolve(FLINK_CONFIG_FILE);
    if (!Files.isRegularFile(flinkConfigFile)) {
      return Optional.empty();
    }

    try {
      return ConfigLoaderUtils.loadFlinkConfig(planDir)
          .getOptional(PipelineOptions.MAX_PARALLELISM);
    } catch (IOException | RuntimeException exception) {
      // Flink reports an invalid YAML file with a plain RuntimeException, and an invalid value
      // with an IllegalArgumentException.
      throw cannotRead(flinkConfigFile, exception, errors);
    }
  }

  private static RuntimeException cannotRead(
      Path file, Exception exception, ErrorCollector errors) {
    return errors.exception("%s", "Cannot read %s: %s".formatted(file, exception.getMessage()));
  }

  /** Returns true when the folder holds a Flink file, so the folder is itself a plan folder. */
  static boolean hasFlinkFiles(Path folder) {
    return Files.isRegularFile(folder.resolve(COMPILED_PLAN_FILE))
        || Files.isRegularFile(folder.resolve(FLINK_CONFIG_FILE));
  }
}
