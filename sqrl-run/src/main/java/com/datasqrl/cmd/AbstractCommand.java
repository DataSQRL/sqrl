/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.cmd;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorPrinter;
import com.datasqrl.graphql.GraphQLServer;
import com.datasqrl.graphql.server.Model.RootGraphqlModel;
import com.datasqrl.io.jdbc.JdbcDataSystemConnectorConfig;
import io.vertx.core.Vertx;
import io.vertx.jdbcclient.JDBCConnectOptions;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.PgPool;
import io.vertx.pgclient.impl.PgPoolOptions;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.SqlClient;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine;

import java.util.concurrent.CompletableFuture;

@Slf4j
public abstract class AbstractCommand implements Runnable {

  @CommandLine.ParentCommand
  protected RootCommand root;

  @SneakyThrows
  public void run() {
    ErrorCollector collector = ErrorCollector.root();
    try {
      runCommand(collector);
      root.statusHook.onSuccess();
    } catch (Exception e) {
      collector.getCatcher().handle(e);
      e.printStackTrace();
      root.statusHook.onFailure();
    }
    System.out.println(ErrorPrinter.prettyPrint(collector));
  }

  protected abstract void runCommand(ErrorCollector errors) throws Exception;

  @SneakyThrows
  public void startGraphQLServer(
      RootGraphqlModel model, int port, JdbcDataSystemConnectorConfig jdbc) {
    Vertx vertx = Vertx.vertx();
    SqlClient client;
    if (jdbc.getDialect().equalsIgnoreCase("postgres")) {
      client = PgPool.client(vertx, toPgOptions(jdbc), new PgPoolOptions(new PoolOptions()));
    } else {
      client = JDBCPool.pool(vertx,
          new JDBCConnectOptions()
              .setJdbcUrl(jdbc.getDbURL())
              .setDatabase(jdbc.getDatabase()),
          new PoolOptions());
    }

    CompletableFuture future = vertx.deployVerticle(new GraphQLServer(
            model, port, client))
        .toCompletionStage()
        .toCompletableFuture();
    log.info("Server started at: http://localhost:" + port + "/graphiql/");
    future.get();
  }

  private PgConnectOptions toPgOptions(JdbcDataSystemConnectorConfig jdbcConf) {
    PgConnectOptions options = new PgConnectOptions();
    options.setDatabase(jdbcConf.getDatabase());
    options.setHost(jdbcConf.getHost());
    options.setPort(jdbcConf.getPort());
    options.setUser(jdbcConf.getUser());
    options.setPassword(jdbcConf.getPassword());
    options.setCachePreparedStatements(true);
    options.setPipeliningLimit(100_000);
    return options;
  }
}
