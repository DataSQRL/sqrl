package com.datasqrl.engine.server;

import static com.datasqrl.engine.EngineCapability.NO_CAPABILITIES;

import com.datasqrl.config.SqrlConfig;
import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.engine.ExecutionResult;
import com.datasqrl.engine.ExecutionResult.Message;
import com.datasqrl.engine.database.relational.JDBCEngine;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.graphql.GraphQLServer;
import com.datasqrl.graphql.server.Model.KafkaMutationCoords;
import com.datasqrl.graphql.server.Model.RootGraphqlModel;
import com.datasqrl.io.tables.TableSink;
import com.datasqrl.plan.global.PhysicalDAGPlan.StagePlan;
import com.datasqrl.plan.global.PhysicalDAGPlan.StageSink;
import com.datasqrl.serializer.Deserializer;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import io.vertx.core.Vertx;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.tools.RelBuilder;

/**
 * A no-feature java server engine.
 */
@Slf4j
public abstract class GenericJavaServerEngine extends ExecutionEngine.Base implements ServerEngine {

  public static final String PORT_KEY = "port";
  public static final int PORT_DEFAULT = 8888;

  private final int port;

  public GenericJavaServerEngine(String engineName, @NonNull SqrlConfig config) {
    super(engineName, Type.SERVER, NO_CAPABILITIES);
    this.port = config.asInt(PORT_KEY).withDefault(PORT_DEFAULT).get();
  }

  @Override
  public CompletableFuture<ExecutionResult> execute(EnginePhysicalPlan plan, ErrorCollector errors) {
    Preconditions.checkArgument(plan instanceof ServerPhysicalPlan);
    ServerPhysicalPlan serverPlan = (ServerPhysicalPlan)plan;
    Vertx vertx = Vertx.vertx();
    CompletableFuture<String> future = vertx.deployVerticle(new GraphQLServer(
            serverPlan.getModel(), port, serverPlan.getJdbc()))
        .toCompletionStage()
        .toCompletableFuture();
    log.info("Server started at: http://localhost:" + port + "/graphiql/");
    return future.thenApply(Message::new);
  }

  @Override
  public EnginePhysicalPlan plan(StagePlan plan, List<StageSink> inputs,
      ExecutionPipeline pipeline, RelBuilder relBuilder, TableSink errorSink) {
    Set<ExecutionStage> dbStages = pipeline.getStages().stream().filter(s -> s.getEngine().getType()==Type.DATABASE).collect(
        Collectors.toSet());
    Preconditions.checkArgument(dbStages.size()==1, "Currently only support a single database stage in server");
    ExecutionEngine engine = Iterables.getOnlyElement(dbStages).getEngine();
    Preconditions.checkArgument(engine instanceof JDBCEngine, "Currently the server only supports JDBC databases");
    return new ServerPhysicalPlan(plan.getModel(), ((JDBCEngine)engine).getConnector());
  }

  @Override
  public EnginePhysicalPlan readPlanFrom(Path directory, String stageName, Deserializer deserializer) throws IOException {
    return ServerPhysicalPlan.readFrom(directory, stageName, deserializer);
  }
}
