package com.datasqrl.actions;

import com.datasqrl.config.BuildPath;
import com.datasqrl.config.CompilerConfiguration;
import com.datasqrl.config.CompilerConfiguration.ExplainConfig;
import com.datasqrl.config.TargetPath;
import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.engine.PhysicalPlan;
import com.datasqrl.engine.PhysicalPlan.StagePlan;
import com.datasqrl.engine.database.relational.JDBCPhysicalPlan;
import com.datasqrl.engine.kafka.NewTopic;
import com.datasqrl.engine.server.ServerPhysicalPlan;
import com.datasqrl.engine.stream.flink.plan.FlinkStreamPhysicalPlan;
import com.datasqrl.engine.kafka.KafkaPhysicalPlan;
import com.datasqrl.packager.config.ScriptConfiguration;
import com.datasqrl.graphql.server.Model.RootGraphqlModel;
import com.datasqrl.plan.global.SqrlDAG;
import com.datasqrl.plan.global.SqrlDAGExporter;
import com.datasqrl.plan.global.SqrlDAGExporter.Node;
import com.datasqrl.plan.queries.APISource;
import com.datasqrl.serializer.Deserializer;
import com.datasqrl.sql.SqlDDLStatement;
import com.datasqrl.util.SqrlObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.google.inject.Inject;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
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

  public static final String EXPLAIN_TEXT_FILENAME = "pipeline_explain.txt";
  public static final String EXPLAIN_VISUAL_FILENAME = "pipeline_visual.html";

  public static final String VISUAL_HTML_FILENAME = "visualize_dag.html";

  public static final String DAG_PLACEHOLDER = "${DAG}";

  private final BuildPath buildDir;
  private final TargetPath targetDir;
  private final CompilerConfiguration compilerConfig;
  private final FlinkSqlGenerator generator;

  public void run(Optional<RootGraphqlModel> model, Optional<APISource> graphqlSource, PhysicalPlan physicalPlan, SqrlDAG dag) {
    writeDeployArtifacts(physicalPlan, targetDir);
    graphqlSource.ifPresent(this::writeGraphqlSchema);
    model.ifPresent(this::writeModel);
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
  private void writeModel(RootGraphqlModel model) {
    new Deserializer().writeJson(targetDir.resolve("server-model.json"), model, true);
  }

  @SneakyThrows
  public void writeDeployArtifacts(PhysicalPlan plan, Path deployDir) {
    for (StagePlan stagePlan : plan.getStagePlans()) {
      EnginePhysicalPlan enginePhysicalPlan = stagePlan.getPlan();//.writeTo(deployDir, stagePlan.stage.getName(), serializer);
      if (enginePhysicalPlan instanceof KafkaPhysicalPlan) { //todo service loader
        writeKafka((KafkaPhysicalPlan) enginePhysicalPlan, deployDir);
      } else if (enginePhysicalPlan instanceof JDBCPhysicalPlan) {
        writeJdbc((JDBCPhysicalPlan) enginePhysicalPlan, deployDir);
      } else if (enginePhysicalPlan instanceof ServerPhysicalPlan) {
        writeServer((ServerPhysicalPlan) enginePhysicalPlan, deployDir);
      } else if (enginePhysicalPlan instanceof FlinkStreamPhysicalPlan) {
        writeFlink((FlinkStreamPhysicalPlan) enginePhysicalPlan, deployDir);
      }

    }
//    plan.writeTo(deployDir, new Deserializer());
  }

  @SneakyThrows
  private void writeKafka(KafkaPhysicalPlan plan, Path deployDir) {
    //temporarily write kafka .sh until we migrate to a better mode of creating topics
    Path path = deployDir.resolve("log");
    Files.createDirectories(path);
    StringBuilder b = new StringBuilder();
    b.append("#!/bin/bash");
    //todo write to json
    for (NewTopic topic : plan.getTopics()) {
      b.append("\n");
      b.append(String.format(
          "/opt/bitnami/kafka/bin/kafka-topics.sh --create --bootstrap-server %s " +
              "--topic %s --partitions %s --replication-factor %s",
          plan.getConfig().asString("properties.bootstrap.servers").get(),
          topic.getName(),
            Math.max(topic.getNumPartitions(), 1),
            Math.max(topic.getReplicationFactor(), 1)
          ));
    }
    b.append("\nexit 0;");
    Files.writeString(path.resolve("create-topics.sh"), b.toString());
  }

  @SneakyThrows
  private void writeJdbc(JDBCPhysicalPlan plan, Path deployDir) {
    Path path = deployDir.resolve("database");
    Files.createDirectories(path);
    Path schema = path.resolve("database-schema.sql");
    Files.writeString(schema, plan.getDdlStatements().stream()
        .map(SqlDDLStatement::toSql)
        .collect(Collectors.joining("\n")));
  }

  @SneakyThrows
  private void writeServer(ServerPhysicalPlan plan, Path deployDir) {
    Path path = deployDir.resolve("server");
    Files.createDirectories(path);
    Path model = path.resolve("server-model.json");
    Path config = path.resolve("server-config.json");
    SqrlObjectMapper.INSTANCE.writerWithDefaultPrettyPrinter()
        .writeValue(model.toFile(), plan.getModel());
    String jsonContent = plan.getConfig().toJson().encodePrettily();
    Files.write(config, jsonContent.getBytes(StandardCharsets.UTF_8));
  }

  @SneakyThrows
  private void writeFlink(FlinkStreamPhysicalPlan plan, Path deployDir) {
    Path path = deployDir.resolve("stream");
    Files.createDirectories(path);
    Path sqlPath = path.resolve("flink-plan.sql");
    //1. write sql, flink-config.yaml is passthrough
    List<String> planSql = generator.run(plan);
    Files.writeString(sqlPath, String.join(PLAN_SEPARATOR, planSql));

    //2. write lib folder
    Path lib = path.resolve("lib");
    Files.createDirectories(lib);
    plan.getPlan().getJars()
        .stream()
        .forEach(jar->copyJar(lib, jar));
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
        .resolve("server");
    Files.createDirectories(path);
    writeFile(path
        .resolve(ScriptConfiguration.GRAPHQL_NORMALIZED_FILE_NAME), source.getSchemaDefinition());
  }

  @SneakyThrows
  private void writeFile(Path filePath, String content) {
    Files.deleteIfExists(filePath);
    Files.writeString(filePath, content, StandardOpenOption.CREATE);
  }

}
