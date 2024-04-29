package com.datasqrl.actions;

import com.datasqrl.cmd.EngineKeys;
import com.datasqrl.config.BuildPath;
import com.datasqrl.config.PackageJson.CompilerConfig;
import com.datasqrl.config.PackageJson.ExplainConfig;
import com.datasqrl.config.ScriptConfigImpl;
import com.datasqrl.config.TargetPath;
import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.engine.PhysicalPlan;
import com.datasqrl.engine.PhysicalPlan.StagePlan;
import com.datasqrl.engine.stream.flink.plan.FlinkStreamPhysicalPlan;
import com.datasqrl.plan.global.SqrlDAG;
import com.datasqrl.plan.global.SqrlDAGExporter;
import com.datasqrl.plan.global.SqrlDAGExporter.Node;
import com.datasqrl.plan.queries.APISource;
import com.datasqrl.serializer.Deserializer;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.google.inject.Inject;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;

@AllArgsConstructor(onConstructor_=@Inject)
public class WriteDeploymentArtifacts {
  public static final String PLAN_SEPARATOR = "\n\n";

  public static final String LIB_DIR = "lib";
  public static final String DATA_DIR = "data";
  public static final String EXPLAIN_TEXT_FILENAME = "pipeline_explain.txt";
  public static final String EXPLAIN_VISUAL_FILENAME = "pipeline_visual.html";

  public static final String VISUAL_HTML_FILENAME = "visualize_dag.html";

  public static final String DAG_PLACEHOLDER = "${DAG}";

  private final BuildPath buildDir;
  private final TargetPath targetDir;
  private final CompilerConfig compilerConfig;

  public void run(Optional<APISource> graphqlSource, PhysicalPlan physicalPlan, SqrlDAG dag) {
    writeDeployArtifacts(physicalPlan, targetDir);
    graphqlSource.ifPresent(this::writeGraphqlSchema);
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
  public void writeDeployArtifacts(PhysicalPlan plan, Path deployDir) {
    for (StagePlan stagePlan : plan.getStagePlans()) {
      EnginePhysicalPlan enginePhysicalPlan = stagePlan.getPlan();
      if (enginePhysicalPlan instanceof FlinkStreamPhysicalPlan) {
        writeFlink((FlinkStreamPhysicalPlan) enginePhysicalPlan, deployDir);
      }

    }
  }

  @SneakyThrows
  private void writeFlink(FlinkStreamPhysicalPlan plan, Path deployDir) {
//    Path path = deployDir.resolve(EngineKeys.STREAMS);
//    Files.createDirectories(path);
    //Todo: generate before due to bug in dag exporter (relnodes are mutated somewhere)
    // Remove this line later
//    generator.run(plan.getPlan());

    //2. write lib folder
//    Path lib = path.resolve(LIB_DIR);
//    Files.createDirectories(lib);
    //todo copy all jars in lib dir
//    plan.getPlan().getJars()
//        .stream()
//        .forEach(jar->copyJar(lib, jar));

    //Write data dir
//    Path deployDataPath = path.resolve(DATA_DIR);
//    Files.createDirectories(deployDataPath);
//    Path buildDataPath = buildDir.resolve(DATA_DIR);
//    if (Files.isDirectory(buildDataPath)) {
//      Files.move(buildDataPath, deployDataPath, StandardCopyOption.REPLACE_EXISTING);
//    }
  }

  private void copyJar(Path lib, URL jar) {
    String fileName = Paths.get(jar.getPath()).getFileName().toString();

    // Resolve the target file path in the lib directory
    Path targetPath = lib.resolve(fileName);

    // Open a stream from the URL and copy the content to the target path
    try (InputStream in = jar.openStream()) {
      Files.copy(in, targetPath, StandardCopyOption.REPLACE_EXISTING);
    } catch (IOException e) {
      throw new RuntimeException("Failed to copy JAR file: " + jar + " to " + targetPath, e);
    }
  }

  @SneakyThrows
  public void writeGraphqlSchema(APISource source) {
    Path path = buildDir
        .resolve(EngineKeys.SERVER);
    Files.createDirectories(path);
    writeFile(path
        .resolve(ScriptConfigImpl.GRAPHQL_NORMALIZED_FILE_NAME), source.getSchemaDefinition());
  }

  @SneakyThrows
  private void writeFile(Path filePath, String content) {
    Files.deleteIfExists(filePath);
    Files.writeString(filePath, content, StandardOpenOption.CREATE);
  }

}
