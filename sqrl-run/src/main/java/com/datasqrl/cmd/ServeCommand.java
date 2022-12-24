/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.cmd;

import static com.datasqrl.cmd.AbstractCompilerCommand.DEFAULT_DEPLOY_DIR;
import static com.datasqrl.cmd.AbstractCompilerCommand.DEFAULT_SERVER_MODEL;

import com.datasqrl.config.GlobalEngineConfiguration;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.graphql.GraphQLServer;
import com.datasqrl.graphql.server.Model.RootGraphqlModel;
import com.datasqrl.io.jdbc.JdbcDataSystemConnectorConfig;
import com.datasqrl.service.PathUtil;
import com.datasqrl.service.Util;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.Vertx;
import io.vertx.jdbcclient.JDBCConnectOptions;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.PgPool;
import io.vertx.pgclient.impl.PgPoolOptions;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.SqlClient;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine;

@Slf4j
@CommandLine.Command(name = "serve", description = "Serves a graphql api")
public class ServeCommand extends AbstractCommand {

  @CommandLine.Option(names = {"-p", "--port"}, description = "Port for API server")
  private int port = 8888;

  @Override
  protected void runCommand(ErrorCollector errors) throws Exception {
    //Get jdbc config from package.json
    List<Path> packageFiles = PathUtil.getOrCreateDefaultPackageFiles(root);
    GlobalEngineConfiguration engineConfig = GlobalEngineConfiguration.readFrom(packageFiles,
        GlobalEngineConfiguration.class);

    startGraphQLServer(readModel(), port, Util.getJdbcEngine(engineConfig.getEngines()));
  }

  @SneakyThrows
  private RootGraphqlModel readModel() {
    Path outputDir = root.rootDir.resolve(DEFAULT_DEPLOY_DIR);
    ObjectMapper mapper = new ObjectMapper();
    return mapper.readValue(outputDir.resolve(DEFAULT_SERVER_MODEL).toFile(), RootGraphqlModel.class);
  }
}
