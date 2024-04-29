package com.datasqrl.actions;

import com.datasqrl.config.BuildPath;
import com.datasqrl.config.PackageJson.CompilerConfig;
import com.datasqrl.config.PackageJson.ExplainConfig;
import com.datasqrl.plan.global.SqrlDAG;
import com.datasqrl.plan.global.SqrlDAGExporter;
import com.datasqrl.plan.global.SqrlDAGExporter.Node;
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

@AllArgsConstructor(onConstructor_=@Inject)
public class WriteDag {

  public static final String LIB_DIR = "lib";
  public static final String DATA_DIR = "data";
  public static final String EXPLAIN_TEXT_FILENAME = "pipeline_explain.txt";
  public static final String EXPLAIN_VISUAL_FILENAME = "pipeline_visual.html";

  public static final String VISUAL_HTML_FILENAME = "visualize_dag.html";

  public static final String DAG_PLACEHOLDER = "${DAG}";

  private final BuildPath buildDir;
  private final CompilerConfig compilerConfig;

  public void run(SqrlDAG dag) {
    writeExplain(dag);
  }

  @SneakyThrows
  private void writeExplain(SqrlDAG dag) {
    ExplainConfig explainConfig = compilerConfig.getExplain();
    if (explainConfig.isText()) {
      SqrlDAGExporter exporter = SqrlDAGExporter.builder()
          .includeQueries(false)
          .includeImports(false)
          .withHints(explainConfig.isExtended())
          .build();
      List<Node> nodes = exporter.export(dag);
      if (explainConfig.isSorted()) Collections.sort(nodes); //make order deterministic
      writeFile(buildDir.resolve(EXPLAIN_TEXT_FILENAME),nodes.stream().map(SqrlDAGExporter.Node::toString)
          .collect(Collectors.joining("\n")));
    }
    if (explainConfig.isVisual()) {
      SqrlDAGExporter exporter = SqrlDAGExporter.builder()
          .includeQueries(true)
          .includeImports(true)
          .withHints(explainConfig.isExtended())
          .build();
      List<Node> nodes = exporter.export(dag);
      if (explainConfig.isSorted()) Collections.sort(nodes); //make order deterministic
      String jsonContent = new Deserializer().toJson(nodes);
      String htmlFile = Resources.toString(Resources.getResource(VISUAL_HTML_FILENAME), Charsets.UTF_8);
      htmlFile = htmlFile.replace(DAG_PLACEHOLDER, jsonContent);
      writeFile(buildDir.resolve(EXPLAIN_VISUAL_FILENAME),htmlFile);
    }
  }

  @SneakyThrows
  private void writeFile(Path filePath, String content) {
    Files.deleteIfExists(filePath);
    Files.writeString(filePath, content, StandardOpenOption.CREATE);
  }
}
