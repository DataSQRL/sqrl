package com.datasqrl.engine.server;

import static com.datasqrl.engine.EngineFeature.NO_CAPABILITIES;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.config.ConnectorConf;
import com.datasqrl.config.EngineFactory.Type;
import com.datasqrl.config.JdbcDialect;
import com.datasqrl.config.JdbcEngineConfigDelegate;
import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.engine.database.relational.AbstractJDBCEngine;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.graphql.config.CorsHandlerOptions;
import com.datasqrl.graphql.config.ServerConfig;
import com.datasqrl.plan.global.PhysicalDAGPlan.ServerStagePlan;
import com.datasqrl.plan.global.PhysicalDAGPlan.StagePlan;
import com.datasqrl.plan.global.PhysicalDAGPlan.StageSink;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.handler.graphql.GraphiQLHandlerOptions;
import io.vertx.pgclient.PgConnectOptions;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

/**
 * A generic java server engine.
 */
@Slf4j
public abstract class GenericJavaServerEngine extends ExecutionEngine.Base implements ServerEngine {

  public static final String PORT_KEY = "port";
  public static final int PORT_DEFAULT = 8888;


  private static final NameCanonicalizer canonicalize = NameCanonicalizer.SYSTEM;

  public GenericJavaServerEngine(String engineName) {
    super(engineName, Type.SERVER, NO_CAPABILITIES);
  }

  @Override
  public EnginePhysicalPlan plan(StagePlan plan, List<StageSink> inputs, ExecutionPipeline pipeline,
      SqrlFramework relBuilder, ErrorCollector errorCollector) {
    Preconditions.checkArgument(plan instanceof ServerStagePlan);
    Set<ExecutionStage> dbStages = pipeline.getStages().stream().filter(s -> s.getEngine().getType()== Type.DATABASE).collect(
        Collectors.toSet());
    Preconditions.checkArgument(dbStages.size()==1, "Currently only support a single database stage in server");
    ExecutionEngine engine = Iterables.getOnlyElement(dbStages).getEngine();
    Preconditions.checkArgument(engine instanceof AbstractJDBCEngine, "Currently the server only supports JDBC databases");

    //todo: move to template

    ServerConfig updatedConfig = applyDefaults(new ServerConfig(JsonObject.of()), ((AbstractJDBCEngine)engine).getConnectorFactoryConfig());
    return new ServerPhysicalPlan(null, updatedConfig);
  }

  public static ServerConfig applyDefaults(ServerConfig serverConfig,
      ConnectorConf connectorConf) {
    if (serverConfig.getPgConnectOptions() == null) {
      serverConfig.setPgConnectOptions(new PgConnectOptions(new JsonObject()));
    }
    PgConnectOptions pgConnect = serverConfig.getPgConnectOptions();
    pgConnect.setHost("${PGHOST}");
    pgConnect.setPort(5432);
    pgConnect.setUser("${PGUSER}");
    pgConnect.setPassword("${PGPASSWORD}");
    pgConnect.setDatabase("${PGDATABASE}");

    if (serverConfig.getGraphiQLHandlerOptions() == null) {
      GraphiQLHandlerOptions graphiql = new GraphiQLHandlerOptions(new JsonObject());
      graphiql.setEnabled(true);
      serverConfig.setGraphiQLHandlerOptions(graphiql);
    }

    HttpServerOptions http = serverConfig.getHttpServerOptions();
    http.setPort(8888);
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
