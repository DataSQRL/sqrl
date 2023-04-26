/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.cmd;

import com.datasqrl.compile.Compiler;
import com.datasqrl.compile.Compiler.CompilerResult;
import com.datasqrl.config.PipelineFactory;
import com.datasqrl.config.SqrlConfig;
import com.datasqrl.engine.PhysicalPlan;
import com.datasqrl.engine.PhysicalPlanExecutor;
import com.datasqrl.engine.database.DatabaseEngine;
import com.datasqrl.engine.database.relational.JDBCEngine;
import com.datasqrl.error.ErrorCode;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.graphql.APIType;
import com.datasqrl.io.impl.jdbc.JdbcDataSystemConnector;
import com.datasqrl.packager.Packager;
import com.datasqrl.packager.config.ScriptConfiguration;
import com.datasqrl.serializer.Deserializer;
import com.datasqrl.service.Build;
import com.datasqrl.service.PackagerUtil;
import com.google.common.base.Preconditions;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import picocli.CommandLine;
import picocli.CommandLine.ScopeType;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

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


  private final Deserializer writer = new Deserializer();

  protected AbstractCompilerCommand(boolean execute, boolean startGraphql) {
    this.execute = execute;
    this.startGraphql = startGraphql;
  }

  @SneakyThrows
  public void runCommand(ErrorCollector errors) throws IOException {
    SqrlConfig config = PackagerUtil.getOrCreateDefaultConfiguration(root, errors);
    Build build = new Build(errors);
    Packager packager = PackagerUtil.create(root.rootDir, files, config, errors);
    Path packageFilePath = build.build(packager, !noinfer);
    PipelineFactory pipelineFactory = PipelineFactory.fromRootConfig(config);
    DatabaseEngine dbEngine = pipelineFactory.getDatabaseEngine();
    errors.checkFatal(dbEngine instanceof JDBCEngine, "Expected configured "
        + "database engine to be a JDBC database: %s");
    JdbcDataSystemConnector jdbc = ((JDBCEngine)dbEngine).getConnector();

    Compiler compiler = new Compiler();

    Preconditions.checkArgument(Files.isRegularFile(packageFilePath));

    Compiler.CompilerResult result = compiler.run(errors, packageFilePath.getParent(), debug);
    write(result, jdbc);
    if (generateAPI.length>0) {
      errors.checkFatal(Arrays.stream(generateAPI).noneMatch(a -> a!=APIType.GraphQL), ErrorCode.NOT_YET_IMPLEMENTED, "DataSQRL currently only supports GraphQL APIs.");
      writeSchema(root.getRootDir(), result.getGraphQLSchema());
    }

    Optional<CompletableFuture> fut = Optional.empty();
    if (startGraphql) {
      fut = Optional.of(CompletableFuture.runAsync(()->
          startGraphQLServer(result.getModel(),
              port, jdbc)));
    }

    if (execute) {
      executePlan(result.getPlan(), errors);
    }

    fut.map(f-> {
      try {
        return f.get();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });

    if (errors.isFatal()) {
      throw new RuntimeException("Could not run");
    }
  }

  @SneakyThrows
  private void writeSchema(Path rootDir, String schema) {
    Path schemaFile = rootDir.resolve(ScriptConfiguration.GRAPHQL_NORMALIZED_FILE_NAME);
    Files.deleteIfExists(schemaFile);
    Files.writeString(schemaFile,
        schema, StandardOpenOption.CREATE);
  }

  @SneakyThrows
  public void write(CompilerResult result, JdbcDataSystemConnector jdbc) {
    Path outputDir = targetDir;
    if (Files.isDirectory(outputDir)) {
      FileUtils.cleanDirectory(outputDir.toFile());
    } else {
      Files.createDirectories(outputDir);
    }
    writeTo(result, outputDir);
    writeTo(jdbc, outputDir);
  }

  private void writeTo(JdbcDataSystemConnector jdbc, Path outputDir) throws IOException {
    writer.writeJson(outputDir.resolve(DEFAULT_SERVER_CONFIG), jdbc, true);
  }

  public void writeTo(CompilerResult result, Path outputDir) throws IOException {
    writer.writeJson(outputDir.resolve(DEFAULT_SERVER_MODEL), result.getModel(), true);

    //TODO: Add graphql plan as engine, generate all deployment assets
//    FlinkStreamPhysicalPlan plan = (FlinkStreamPhysicalPlan)result.getPlan().getStagePlans().get(0).getPlan();
//    Files.writeString(outputDir.resolve("flinkPlan.json"),
//        writer.writeValueAsString(plan), StandardOpenOption.CREATE);
  }

  private void executePlan(PhysicalPlan physicalPlan, ErrorCollector errors) {
    PhysicalPlanExecutor executor = new PhysicalPlanExecutor();
    PhysicalPlanExecutor.Result result = executor.execute(physicalPlan, errors);
  }
}
