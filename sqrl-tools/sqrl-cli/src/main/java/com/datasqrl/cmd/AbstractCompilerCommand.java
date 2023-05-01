/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.cmd;

import com.datasqrl.compile.Compiler;
import com.datasqrl.config.PipelineFactory;
import com.datasqrl.config.SqrlConfig;
import com.datasqrl.engine.PhysicalPlan;
import com.datasqrl.engine.PhysicalPlanExecutor;
import com.datasqrl.engine.database.DatabaseEngine;
import com.datasqrl.engine.database.relational.JDBCEngine;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.graphql.APIType;
import com.datasqrl.io.impl.jdbc.JdbcDataSystemConnector;
import com.datasqrl.packager.Packager;
import com.datasqrl.serializer.Deserializer;
import com.datasqrl.service.Build;
import com.datasqrl.service.PackagerUtil;
import com.google.common.base.Preconditions;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine;
import picocli.CommandLine.ScopeType;

@Slf4j
public abstract class AbstractCompilerCommand extends AbstractCommand {

  public static final Path DEFAULT_DEPLOY_DIR = Path.of("build", "deploy");
  protected final boolean execute;
  protected final boolean startGraphql;

  @CommandLine.Parameters(arity = "1..2", description = "Main script and (optional) API specification")
  protected Path[] files;

  @CommandLine.Option(names = {"-a", "--api"}, description = "Generates the API specification for the given type")
  protected APIType[] generateAPI = new APIType[0];

  @CommandLine.Option(names = {"-d", "--debug"}, description = "Outputs table changestream to configured sink for debugging")
  protected boolean debug = false;

  @CommandLine.Option(names = {"-t", "--target"}, description = "Target directory for deployment artifacts")
  protected Path targetDir = DEFAULT_DEPLOY_DIR;

  @CommandLine.Option(names = {"-p","--port"}, description = "Port for API server")
  protected int port = 8888;

  @CommandLine.Option(names = {"--nolookup"}, description = "Do not look up package dependencies in the repository",
      scope = ScopeType.INHERIT)
  protected boolean noinfer = false;

  protected AbstractCompilerCommand(boolean execute, boolean startGraphql) {
    this.execute = execute;
    this.startGraphql = startGraphql;
  }

  public void runCommand(ErrorCollector errors) {
    SqrlConfig config = PackagerUtil.getOrCreateDefaultConfiguration(root, errors);
    Packager packager = PackagerUtil.create(root.rootDir, files, config, errors);

    Build build = new Build(errors);
    Path packageFilePath = build.build(packager, !noinfer);
    PipelineFactory pipelineFactory = PipelineFactory.fromRootConfig(config);
    DatabaseEngine dbEngine = pipelineFactory.getDatabaseEngine();
    errors.checkFatal(dbEngine instanceof JDBCEngine, "Expected configured "
        + "database engine to be a JDBC database: %s");
    JdbcDataSystemConnector jdbc = ((JDBCEngine)dbEngine).getConnector();

    Compiler compiler = new Compiler();

    Preconditions.checkArgument(Files.isRegularFile(packageFilePath));

    Compiler.CompilerResult result = compiler.run(errors, packageFilePath.getParent(), debug, targetDir);

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

  private void executePlan(PhysicalPlan physicalPlan, ErrorCollector errors) {
    PhysicalPlanExecutor executor = new PhysicalPlanExecutor();
    PhysicalPlanExecutor.Result result = executor.execute(physicalPlan, errors);
  }
}
