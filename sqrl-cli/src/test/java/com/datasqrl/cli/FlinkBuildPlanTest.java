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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datasqrl.error.CollectedException;
import com.datasqrl.error.ErrorCollector;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FlinkBuildPlanTest {

  private static final String EMPTY_COMPILED_PLAN = "{\"flinkVersion\": \"2.3\", \"nodes\": []}";

  @TempDir private Path planDir;

  @Test
  void givenCompiledPlanAndMaxParallelism_whenRead_thenReadsFlinkJobPlan() throws IOException {
    writeCompiledPlan(EMPTY_COMPILED_PLAN);
    writeFlinkConfig("pipeline.max-parallelism: 144\n");

    var flinkBuildPlan = FlinkBuildPlan.read(planDir, ErrorCollector.root());

    assertThat(flinkBuildPlan.planDir()).isEqualTo(planDir);
    assertThat(flinkBuildPlan.flinkJobPlan())
        .hasValueSatisfying(
            flinkJobPlan -> {
              assertThat(flinkJobPlan.compiledPlan().path("flinkVersion").asText())
                  .isEqualTo("2.3");
              assertThat(flinkJobPlan.maxParallelism()).contains(144);
            });
  }

  @Test
  void givenFlinkConfigWithoutMaxParallelism_whenRead_thenMaxParallelismEmpty() throws IOException {
    writeCompiledPlan(EMPTY_COMPILED_PLAN);
    writeFlinkConfig("parallelism.default: 2\n");

    assertThat(FlinkBuildPlan.read(planDir, ErrorCollector.root()).flinkJobPlan())
        .hasValueSatisfying(flinkJobPlan -> assertThat(flinkJobPlan.maxParallelism()).isEmpty());
  }

  @Test
  void givenPlanDirWithoutFlinkFiles_whenRead_thenNoFlinkJobPlan() throws IOException {
    Files.writeString(planDir.resolve("postgres.json"), "{}");

    assertThat(FlinkBuildPlan.read(planDir, ErrorCollector.root()).flinkJobPlan()).isEmpty();
  }

  @Test
  void givenFlinkConfigWithoutCompiledPlan_whenRead_thenNoFlinkJobPlan() throws IOException {
    writeFlinkConfig("pipeline.max-parallelism: 144\n");

    assertThat(FlinkBuildPlan.read(planDir, ErrorCollector.root()).flinkJobPlan()).isEmpty();
  }

  @Test
  void givenFolderWithCompiledPlan_whenHasFlinkFiles_thenTrue() throws IOException {
    writeCompiledPlan("{}");

    assertThat(FlinkBuildPlan.hasFlinkFiles(planDir)).isTrue();
  }

  @Test
  void givenFolderWithoutFlinkFiles_whenHasFlinkFiles_thenFalse() {
    assertThat(FlinkBuildPlan.hasFlinkFiles(planDir)).isFalse();
  }

  @Test
  void givenEmptyCompiledPlanFile_whenRead_thenFatalNotACompiledPlan() throws IOException {
    writeCompiledPlan("");

    assertThatThrownBy(() -> FlinkBuildPlan.read(planDir, ErrorCollector.root()))
        .isInstanceOf(CollectedException.class)
        .hasMessageContaining("is not a compiled Flink plan");
  }

  @Test
  void givenCompiledPlanWithoutNodes_whenRead_thenFatalNotACompiledPlan() throws IOException {
    writeCompiledPlan("{\"flinkVersion\": \"2.3\"}");

    assertThatThrownBy(() -> FlinkBuildPlan.read(planDir, ErrorCollector.root()))
        .isInstanceOf(CollectedException.class)
        .hasMessageContaining("is not a compiled Flink plan");
  }

  @Test
  void givenInvalidJson_whenRead_thenFatalWithFilePath() throws IOException {
    writeCompiledPlan("{\"nodes\": [");

    assertThatThrownBy(() -> FlinkBuildPlan.read(planDir, ErrorCollector.root()))
        .isInstanceOf(CollectedException.class)
        .hasMessageContaining("Cannot read " + planDir.resolve("flink-compiled-plan.json"));
  }

  @Test
  void givenInvalidYaml_whenRead_thenFatalWithFilePath() throws IOException {
    writeCompiledPlan(EMPTY_COMPILED_PLAN);
    writeFlinkConfig("pipeline.max-parallelism: [unclosed\n");

    assertThatThrownBy(() -> FlinkBuildPlan.read(planDir, ErrorCollector.root()))
        .isInstanceOf(CollectedException.class)
        .hasMessageContaining("Cannot read " + planDir.resolve("flink-config.yaml"));
  }

  @Test
  void givenInvalidMaxParallelism_whenRead_thenFatalWithFilePath() throws IOException {
    writeCompiledPlan(EMPTY_COMPILED_PLAN);
    writeFlinkConfig("pipeline.max-parallelism: abc\n");

    assertThatThrownBy(() -> FlinkBuildPlan.read(planDir, ErrorCollector.root()))
        .isInstanceOf(CollectedException.class)
        .hasMessageContaining("Cannot read " + planDir.resolve("flink-config.yaml"))
        .hasMessageContaining("abc");
  }

  private void writeCompiledPlan(String content) throws IOException {
    Files.writeString(planDir.resolve("flink-compiled-plan.json"), content);
  }

  private void writeFlinkConfig(String content) throws IOException {
    Files.writeString(planDir.resolve("flink-config.yaml"), content);
  }
}
