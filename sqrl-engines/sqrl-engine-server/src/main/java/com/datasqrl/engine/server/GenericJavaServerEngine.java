package com.datasqrl.engine.server;

import static com.datasqrl.engine.EngineCapability.NO_CAPABILITIES;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.config.Constraints.Default;
import com.datasqrl.config.Constraints.MinLength;
import com.datasqrl.config.Constraints.Regex;
import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.engine.ExecutionResult;
import com.datasqrl.engine.ExecutionResult.Message;
import com.datasqrl.engine.database.relational.JDBCEngine;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.graphql.GraphQLServer;
import com.datasqrl.graphql.config.CorsHandlerOptions;
import com.datasqrl.graphql.config.ServerConfig;
import com.datasqrl.io.impl.jdbc.JdbcDataSystemConnector;
import com.datasqrl.io.tables.TableSink;
import com.datasqrl.plan.global.PhysicalDAGPlan.ServerStagePlan;
import com.datasqrl.plan.global.PhysicalDAGPlan.StagePlan;
import com.datasqrl.plan.global.PhysicalDAGPlan.StageSink;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.jdbcclient.JDBCConnectOptions;
import io.vertx.pgclient.PgConnectOptions;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 * A generic java server engine.
 */
@Slf4j
public abstract class GenericJavaServerEngine extends ExecutionEngine.Base implements ServerEngine {

  public static final String PORT_KEY = "port";
  public static final int PORT_DEFAULT = 8888;

  private final int port;
  private final ServerConfig serverConfig;
  private final Optional<Vertx> vertx;

  private static final NameCanonicalizer canonicalize = NameCanonicalizer.SYSTEM;

  public GenericJavaServerEngine(String engineName, int port, @NonNull ServerConfig serverConfig, Optional<Vertx> vertx) {
    super(engineName, Type.SERVER, NO_CAPABILITIES);
    this.port = port;
    this.serverConfig = serverConfig;
    this.vertx = vertx;
  }

  @Override
  public CompletableFuture<ExecutionResult> execute(EnginePhysicalPlan plan, ErrorCollector errors) {
    Preconditions.checkArgument(plan instanceof ServerPhysicalPlan);
    ServerPhysicalPlan serverPlan = (ServerPhysicalPlan)plan;
    Vertx vertx = this.vertx.orElseGet(Vertx::vertx);
    CompletableFuture<String> future = vertx.deployVerticle(new GraphQLServer(
            serverPlan.getModel(), serverPlan.getConfig(), canonicalize))
        .toCompletionStage()
        .toCompletableFuture();
    if (serverPlan.getConfig().getGraphiQLHandlerOptions().isEnabled()) {
      log.info(String.format("Server started at: %s://%s:%s/%s",
          serverPlan.getConfig().getHttpServerOptions().isSsl() ? "https" : "http",
          serverPlan.getConfig().getServletConfig().getGraphiQLEndpoint(),
          serverPlan.getConfig().getHttpServerOptions().getPort(),
          serverPlan.getConfig().getServletConfig().getGraphiQLEndpoint()));
    }
    return future.thenApply(Message::new);
  }

  @Override
  public EnginePhysicalPlan plan(StagePlan plan, List<StageSink> inputs, ExecutionPipeline pipeline,
      SqrlFramework relBuilder, TableSink errorSink) {
    Preconditions.checkArgument(plan instanceof ServerStagePlan);
    Set<ExecutionStage> dbStages = pipeline.getStages().stream().filter(s -> s.getEngine().getType()==Type.DATABASE).collect(
        Collectors.toSet());
    Preconditions.checkArgument(dbStages.size()==1, "Currently only support a single database stage in server");
    ExecutionEngine engine = Iterables.getOnlyElement(dbStages).getEngine();
    Preconditions.checkArgument(engine instanceof JDBCEngine, "Currently the server only supports JDBC databases");

    ServerConfig updatedConfig = applyDefaults(serverConfig, ((JDBCEngine)engine).getConnector(), this.port);
    return new ServerPhysicalPlan(((ServerStagePlan) plan).getModel(), updatedConfig);
  }

  public static ServerConfig applyDefaults(ServerConfig serverConfig, JdbcDataSystemConnector connector, int port) {
    if (connector.getDialect().equals("postgres")) {
      if (serverConfig.getPgConnectOptions() == null) {
        serverConfig.setPgConnectOptions(new PgConnectOptions());
      }
      PgConnectOptions pgConnect = serverConfig.getPgConnectOptions();

      if (connector.getHost() != null) {
        pgConnect.setHost(connector.getHost());
      }
      if (connector.getPort() != null) {
        pgConnect.setPort(connector.getPort());
      }
      if (connector.getUser() != null) {
        pgConnect.setUser(connector.getUser());
      }
      if (connector.getPassword() != null) {
        pgConnect.setPassword(connector.getPassword());
      }
      if (connector.getDatabase() != null) {
        pgConnect.setDatabase(connector.getDatabase());
      }
    }

    if (serverConfig.getJdbcConnectOptions() == null) {
      serverConfig.setJdbcConnectOptions(new JDBCConnectOptions());
    }
    JDBCConnectOptions jdbc = serverConfig.getJdbcConnectOptions();
    if (connector.getUrl() != null) {
      jdbc.setJdbcUrl(connector.getUrl());
    }
    if (connector.getUser() != null) {
      jdbc.setUser(connector.getUser());
    }
    if (connector.getPassword() != null) {
      jdbc.setPassword(connector.getPassword());
    }
    if (connector.getDatabase() != null) {
      jdbc.setDatabase(connector.getDatabase());
    }

    HttpServerOptions http = serverConfig.getHttpServerOptions();http.setPort(port);
    if (http.getWebSocketSubProtocols() == null) {
      http.setWebSocketSubProtocols(List.of("graphql-transport-ws", "graphql-ws"));
    }

    if (serverConfig.getCorsHandlerOptions().getAllowedOrigin() == null &&
    serverConfig.getCorsHandlerOptions().getAllowedOrigins() == null) {
      CorsHandlerOptions cors = serverConfig.getCorsHandlerOptions();
      cors.setAllowedOrigin("*");
      cors.setAllowedMethods(Set.of("GET", "POST"));
    }

    return serverConfig;
  }
}
