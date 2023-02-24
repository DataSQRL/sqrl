/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.cmd;

import com.datasqrl.compile.Compiler;
import com.datasqrl.compile.Compiler.CompilerResult;
import com.datasqrl.config.GlobalEngineConfiguration;
import com.datasqrl.engine.PhysicalPlan;
import com.datasqrl.engine.PhysicalPlanExecutor;
import com.datasqrl.error.ErrorCode;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.graphql.APIType;
import com.datasqrl.io.jdbc.JdbcDataSystemConnectorConfig;
import com.datasqrl.packager.Packager;
import com.datasqrl.service.Build;
import com.datasqrl.service.PackagerUtil;
import com.datasqrl.service.Util;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import picocli.CommandLine;
import picocli.CommandLine.ScopeType;

@Slf4j
public abstract class AbstractCompilerCommand extends AbstractCommand {

  public static final Path DEFAULT_DEPLOY_DIR = Path.of("deploy");
  public static final String DEFAULT_SERVER_MODEL = "model.json";
  public static final String DEFAULT_SERVER_CONFIG = "config.json";

  private final boolean execute;
  private final boolean startGraphql;

  @CommandLine.Parameters(arity = "1..2", description = "Main script and (optional) API specification")
  private Path[] files;

  @CommandLine.Option(names = {"-a", "--api"}, description = "Generates the API specification for the given type")
  private APIType[] generateAPI = new APIType[0];

  @CommandLine.Option(names = {"-d", "--debug"}, description = "Outputs table changestream to configured sink for debugging")
  private boolean debug = false;

  @CommandLine.Option(names = {"-t", "--target"}, description = "Target directory for deployment artifacts")
  private Path targetDir = DEFAULT_DEPLOY_DIR;

  @CommandLine.Option(names = {"-p","--port"}, description = "Port for API server")
  private int port = 8888;

  @CommandLine.Option(names = {"--nolookup"}, description = "Do not look up package dependencies in the repository",
      scope = ScopeType.INHERIT)
  protected boolean noinfer = false;

  private final ObjectWriter writer = new ObjectMapper()
      .writerWithDefaultPrettyPrinter();

  protected AbstractCompilerCommand(boolean execute, boolean startGraphql) {
    this.execute = execute;
    this.startGraphql = startGraphql;
  }

  @SneakyThrows
  public void runCommand(ErrorCollector collector) throws IOException {
    List<Path> packageFiles = PackagerUtil.getOrCreateDefaultPackageFiles(root);

    Build build = new Build(collector);
    Packager packager = PackagerUtil.create(root.rootDir, files, packageFiles, collector);
    Path buildLoc = build.build(packager, !noinfer);

    GlobalEngineConfiguration engineConfig = GlobalEngineConfiguration.readFrom(packageFiles,
        GlobalEngineConfiguration.class);
    JdbcDataSystemConnectorConfig jdbc = Util.getJdbcEngine(engineConfig.getEngines());

    Compiler compiler = new Compiler();

    Compiler.CompilerResult result = compiler.run(collector, buildLoc, debug);
    write(result, jdbc);
    if (generateAPI.length>0) {
      collector.checkFatal(Arrays.stream(generateAPI).noneMatch(a -> a!=APIType.GraphQL), ErrorCode.NOT_YET_IMPLEMENTED, "DataSQRL currently only supports GraphQL APIs.");
      writeSchema(root.getRootDir(), result.getGraphQLSchema());
    }

    Optional<CompletableFuture> fut = Optional.empty();
    if (startGraphql) {
      fut = Optional.of(CompletableFuture.runAsync(()->
          startGraphQLServer(result.getModel(),
              port, jdbc)));
    }

    if (execute) {
      executePlan(result.getPlan());
    }

    fut.map(f-> {
      try {
        return f.get();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
  }

  @SneakyThrows
  private void writeSchema(Path rootDir, String schema) {
    Path schemaFile = rootDir.resolve(Packager.GRAPHQL_SCHEMA_FILE_NAME);
    Files.deleteIfExists(schemaFile);
    Files.writeString(schemaFile,
        schema, StandardOpenOption.CREATE);
  }

  @SneakyThrows
  private void write(CompilerResult result, JdbcDataSystemConnectorConfig jdbc) {
    Path outputDir = targetDir;
    if (Files.isDirectory(outputDir)) {
      FileUtils.cleanDirectory(outputDir.toFile());
    } else {
      Files.createDirectories(outputDir);
    }
    writeTo(result, outputDir);
    writeTo(jdbc, outputDir);
  }

  private void writeTo(JdbcDataSystemConnectorConfig jdbc, Path outputDir) throws IOException {
    Files.writeString(outputDir.resolve(DEFAULT_SERVER_CONFIG),
        writer.writeValueAsString(jdbc), StandardOpenOption.CREATE);
  }

  public void writeTo(CompilerResult result, Path outputDir) throws IOException {
    Files.writeString(outputDir.resolve(DEFAULT_SERVER_MODEL),
        writer.writeValueAsString(result.getModel()), StandardOpenOption.CREATE);
  }

  private void executePlan(PhysicalPlan physicalPlan) {
    PhysicalPlanExecutor executor = new PhysicalPlanExecutor();
    PhysicalPlanExecutor.Result result = executor.execute(physicalPlan);
  }
}
