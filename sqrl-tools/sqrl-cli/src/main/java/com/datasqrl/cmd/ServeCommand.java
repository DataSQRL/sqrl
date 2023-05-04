/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.cmd;

import com.datasqrl.config.PipelineFactory;
import com.datasqrl.config.SqrlConfig;
import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.engine.ExecutionEngine.Type;
import com.datasqrl.engine.ExecutionResult;
import com.datasqrl.engine.database.DatabaseEngine;
import com.datasqrl.engine.database.relational.JDBCEngine;
import com.datasqrl.engine.server.ServerEngine;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.graphql.server.Model.RootGraphqlModel;
import com.datasqrl.serializer.Deserializer;
import com.datasqrl.service.PackagerUtil;
import com.datasqrl.util.SqrlObjectMapper;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.concurrent.CompletableFuture;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import picocli.CommandLine;

import java.nio.file.Path;

import static com.datasqrl.cmd.AbstractCompilerCommand.DEFAULT_DEPLOY_DIR;
import static com.datasqrl.compile.Compiler.DEFAULT_SERVER_MODEL;

@Slf4j
@CommandLine.Command(name = "serve", description = "Serves a graphql api")
public class ServeCommand extends AbstractCommand {

  @Override
  protected void runCommand(ErrorCollector errors) throws Exception {
    //Get jdbc config from package.json
    SqrlConfig config = PackagerUtil.getOrCreateDefaultConfiguration(root, errors);
    PipelineFactory pipelineFactory = PipelineFactory.fromRootConfig(config);
    Pair<String, ExecutionEngine> engine = pipelineFactory.getEngine(Type.SERVER);

    ServerEngine serverEngine = (ServerEngine) engine.getValue();
    EnginePhysicalPlan plan = serverEngine.readPlanFrom(root.rootDir.resolve(DEFAULT_DEPLOY_DIR),
        engine.getKey(), new Deserializer());
    CompletableFuture<ExecutionResult> future = serverEngine.execute(plan, errors);
    future.get();
  }
}
