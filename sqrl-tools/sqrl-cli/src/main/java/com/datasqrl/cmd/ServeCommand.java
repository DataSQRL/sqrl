/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.cmd;

import com.datasqrl.config.PipelineFactory;
import com.datasqrl.config.SqrlConfig;
import com.datasqrl.engine.database.DatabaseEngine;
import com.datasqrl.engine.database.relational.JDBCEngine;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.graphql.server.Model.RootGraphqlModel;
import com.datasqrl.service.PackagerUtil;
import com.datasqrl.util.SqrlObjectMapper;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine;

import java.nio.file.Path;

import static com.datasqrl.cmd.AbstractCompilerCommand.DEFAULT_DEPLOY_DIR;
import static com.datasqrl.cmd.AbstractCompilerCommand.DEFAULT_SERVER_MODEL;

@Slf4j
@CommandLine.Command(name = "serve", description = "Serves a graphql api")
public class ServeCommand extends AbstractCommand {

  @CommandLine.Option(names = {"-p","--port"}, description = "Port for API server")
  private int port = 8888;

  @Override
  protected void runCommand(ErrorCollector errors) throws Exception {
    //Get jdbc config from package.json
    SqrlConfig config = PackagerUtil.getOrCreateDefaultConfiguration(root, errors);
    DatabaseEngine dbEngine = PipelineFactory.fromRootConfig(config).getDatabaseEngine();
    errors.checkFatal(dbEngine instanceof JDBCEngine, "Expected configured "
        + "database engine to be a JDBC database: %s");
    startGraphQLServer(readModel(), port, ((JDBCEngine)dbEngine).getConnector());
  }

  @SneakyThrows
  private RootGraphqlModel readModel() {
    Path outputDir = root.rootDir.resolve(DEFAULT_DEPLOY_DIR);
    ObjectMapper mapper = SqrlObjectMapper.INSTANCE;
    return mapper.readValue(outputDir.resolve(DEFAULT_SERVER_MODEL).toFile(), RootGraphqlModel.class);
  }
}
