/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.cmd;

import com.datasqrl.compile.Compiler;
import com.datasqrl.config.provider.JDBCConnectionProvider;
import com.datasqrl.engine.PhysicalPlan;
import com.datasqrl.engine.PhysicalPlanExecutor;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.graphql.GraphQLServer;
import com.datasqrl.graphql.server.Model;
import com.datasqrl.packager.Packager;
import com.google.common.base.Preconditions;
import io.vertx.core.Vertx;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.sqlclient.PoolOptions;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import picocli.CommandLine;

@Slf4j
public abstract class AbstractCompilerCommand extends AbstractCommand {

  public static final String DEFAULT_DEPLOY_DIR = "deploy";

  private final boolean execute;

  @CommandLine.Parameters(arity = "1..2", description = "Main script and (optional) GraphQL schema")
  private Path[] files;

  @CommandLine.Option(names = {"-g", "--generate-schema"}, description = "Generates the graphql "
      + "schema file and exits")
  private boolean generateSchema = false;

  @CommandLine.Option(names = {"-p", "--port"}, description = "Port for API server")
  private int port = 8888;

  @CommandLine.Option(names = {"-o", "--output-dir"}, description = "Output directory")
  private Path outputDir = null;

  protected AbstractCompilerCommand(boolean execute) {
    this.execute = execute;
  }

  public void runCommand(ErrorCollector collector) throws IOException {
    List<Path> packageFiles = getPackageFilesWithDefault(execute);

    Packager.Config.ConfigBuilder pkgBuilder = Packager.Config.builder();
    pkgBuilder.rootDir(root.rootDir);
    pkgBuilder.packageFiles(packageFiles);
    Path mainScript = files[0];
    Preconditions.checkArgument(mainScript != null && Files.isRegularFile(mainScript),
        "Could not find main script: %s", mainScript);
    pkgBuilder.mainScript(mainScript);
    if (files.length > 1) {
      pkgBuilder.graphQLSchemaFile(files[1]);
      generateSchema = false; //We don't generate schema even if the flag is set
    }
    Packager packager = pkgBuilder.build().getPackager();
    packager.cleanUp();
    Path buildPackageFile = packager.populateBuildDir(true);

    Compiler compiler = new Compiler();
    Compiler.CompilerResult result = compiler.run(collector, buildPackageFile);

    if (outputDir == null) {
      outputDir = root.rootDir.resolve(DEFAULT_DEPLOY_DIR);
    }
    if (Files.isDirectory(outputDir)) {
      FileUtils.cleanDirectory(outputDir.toFile());
    } else {
      Files.createDirectories(outputDir);
    }
    result.writeTo(outputDir);

    if (generateSchema) {
      //Copy file from output directory to this directory
      Files.copy(outputDir.resolve(Packager.GRAPHQL_SCHEMA_FILE_NAME),
          Path.of(Packager.GRAPHQL_SCHEMA_FILE_NAME),
          StandardCopyOption.REPLACE_EXISTING);
    }

    if (!execute) {
      return;
    }
    //execute flink
    executePlan(result.getPlan());
    //execute graphql server
    startGraphQLServer(result.getModel(), port, jdbcConnection);
  }


  private void executePlan(PhysicalPlan physicalPlan) {
    PhysicalPlanExecutor executor = new PhysicalPlanExecutor();
    PhysicalPlanExecutor.Result result = executor.execute(physicalPlan);
  }

  @SneakyThrows
  private void startGraphQLServer(Model.RootGraphqlModel model, int port, JDBCConnectionProvider jdbcConf) {
    CompletableFuture future = Vertx.vertx().deployVerticle(new GraphQLServer(
            model, toPgOptions(jdbcConf), port, new PoolOptions()))
        .toCompletionStage()
        .toCompletableFuture();
    log.info("Server started at: http://localhost:"+port+"/graphiql/");
    future.get();
  }

  private PgConnectOptions toPgOptions(JDBCConnectionProvider jdbcConf) {
    PgConnectOptions options = new PgConnectOptions();
    options.setDatabase(jdbcConf.getDatabaseName());
    options.setHost(jdbcConf.getHost());
    options.setPort(jdbcConf.getPort());
    options.setUser(jdbcConf.getUser());
    options.setPassword(jdbcConf.getPassword());
    options.setCachePreparedStatements(true);
    options.setPipeliningLimit(100_000);
    return options;
  }

}
