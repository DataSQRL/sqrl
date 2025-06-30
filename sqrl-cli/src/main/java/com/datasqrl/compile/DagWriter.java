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
package com.datasqrl.compile;

import com.datasqrl.config.BuildPath;
import com.datasqrl.config.PackageJson.CompilerConfig;
import com.datasqrl.config.PackageJson.ExplainConfig;
import com.datasqrl.plan.global.PipelineDAGExporter;
import com.datasqrl.planner.dag.PipelineDAG;
import com.datasqrl.serializer.Deserializer;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.google.inject.Inject;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;

@AllArgsConstructor(onConstructor_ = @Inject)
public class DagWriter {

  public static final String EXPLAIN_TEXT_FILENAME = "pipeline_explain.txt";
  public static final String EXPLAIN_VISUAL_FILENAME = "pipeline_visual.html";
  public static final String EXPLAIN_JSON_FILENAME = "pipeline_explain.json";

  public static final String VISUAL_HTML_FILENAME = "visualize_dag.html";

  public static final String DAG_PLACEHOLDER = "${DAG}";

  private final BuildPath buildDir;
  private final CompilerConfig compilerConfig;

  public void run(PipelineDAG dag) {
    writeExplain(dag);
  }

  @SneakyThrows
  private void writeExplain(PipelineDAG dag) {
    ExplainConfig explainConfig = compilerConfig.getExplain();
    if (explainConfig.isText()) {
      PipelineDAGExporter exporter =
          PipelineDAGExporter.builder()
              .includeQueries(false)
              .includeImports(false)
              .withHints(true)
              .includeLogicalPlan(explainConfig.isLogical())
              .includeSQL(explainConfig.isSql())
              .includePhysicalPlan(explainConfig.isPhysical())
              .build();
      List<PipelineDAGExporter.Node> nodes = exporter.export(dag);
      if (explainConfig.isSorted()) Collections.sort(nodes); // make order deterministic
      writeFile(
          buildDir.getBuildDir().resolve(EXPLAIN_TEXT_FILENAME),
          nodes.stream().map(PipelineDAGExporter.Node::toString).collect(Collectors.joining("\n")));
    }
    if (explainConfig.isVisual()) {
      PipelineDAGExporter exporter =
          PipelineDAGExporter.builder()
              .includeQueries(true)
              .includeImports(true)
              .withHints(true)
              .includeLogicalPlan(true)
              .includeSQL(true)
              .includePhysicalPlan(true)
              .build();
      List<PipelineDAGExporter.Node> nodes = exporter.export(dag);
      if (explainConfig.isSorted()) Collections.sort(nodes); // make order deterministic
      String jsonContent = Deserializer.INSTANCE.toJson(nodes);
      String htmlFile =
          Resources.toString(Resources.getResource(VISUAL_HTML_FILENAME), Charsets.UTF_8);
      htmlFile = htmlFile.replace(DAG_PLACEHOLDER, jsonContent);
      writeFile(buildDir.getBuildDir().resolve(EXPLAIN_VISUAL_FILENAME), htmlFile);
      writeFile(buildDir.getBuildDir().resolve(EXPLAIN_JSON_FILENAME), jsonContent);
    }
  }

  @SneakyThrows
  private void writeFile(Path filePath, String content) {
    Files.deleteIfExists(filePath);
    Files.writeString(filePath, content, StandardOpenOption.CREATE);
  }
}
