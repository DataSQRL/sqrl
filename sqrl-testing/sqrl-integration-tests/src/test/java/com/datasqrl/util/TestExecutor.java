package com.datasqrl.util;

import com.datasqrl.FlinkExecutablePlan;
import com.datasqrl.config.SqrlConfig;
import com.datasqrl.config.SqrlConfigCommons;
import com.datasqrl.engine.ExecutionResult;
import com.datasqrl.engine.database.relational.JDBCEngine;
import com.datasqrl.engine.database.relational.JDBCEngineFactory;
import com.datasqrl.engine.database.relational.JDBCPhysicalPlan;
import com.datasqrl.engine.database.relational.ddl.SqlDDLStatement;
import com.datasqrl.engine.server.ServerPhysicalPlan;
import com.datasqrl.engine.server.VertxEngineFactory;
import com.datasqrl.engine.stream.flink.ExecutionEnvironmentFactory;
import com.datasqrl.engine.stream.flink.LocalFlinkStreamEngineImpl;
import com.datasqrl.engine.stream.flink.plan.FlinkStreamPhysicalPlan;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.graphql.server.Model;
import com.datasqrl.io.impl.jdbc.JdbcDataSystemConnector;
import com.datasqrl.kafka.KafkaLogEngine;
import com.datasqrl.kafka.KafkaLogEngineFactory;
import com.datasqrl.kafka.KafkaPhysicalPlan;
import com.datasqrl.kafka.NewTopic;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.Vertx;
import lombok.SneakyThrows;
import org.apache.logging.log4j.util.Strings;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class TestExecutor {

  private final Vertx vertx;

  public TestExecutor(Vertx vertx) {
    this.vertx = vertx;
  }

  //TODO: Migrate to pipeline deserializer
  @SneakyThrows
  public CompletableFuture<ExecutionResult> executePipeline(Path rootDir) {
    ObjectMapper mapper = SqrlObjectMapper.INSTANCE;

    SqrlConfig config = SqrlConfigCommons.fromFiles(ErrorCollector.root(),
        rootDir.resolve("build").resolve("package.json"));

    Path deployDir = rootDir.resolve("build").resolve("deploy");

    SqrlConfig eng = config.getSubConfig("engines");
    List<SqlDDLStatement> schema = Arrays.stream(
            Files.readString(deployDir.resolve("database-schema.sql")).split("\n"))
        .filter(s -> !Strings.isEmpty(s)).map(s -> (SqlDDLStatement) () -> s)
        .collect(Collectors.toList());
    JDBCPhysicalPlan jdbcPhysicalPlan = new JDBCPhysicalPlan(schema, Map.of(), Map.of());
    JDBCEngineFactory jdbcEngineFactory = new JDBCEngineFactory();
    JDBCEngine jdbcEngine = jdbcEngineFactory.initialize(eng.getSubConfig("database"));
    jdbcEngine.execute(jdbcPhysicalPlan, ErrorCollector.root()).get();

    List<NewTopic> kakfaPlan = mapper.readValue(deployDir.resolve("kafka-plan.json").toFile(),
        new TypeReference<List<NewTopic>>() {
        });
    KafkaLogEngineFactory logEngineFactory = new KafkaLogEngineFactory();
    KafkaLogEngine kafkaLogEngine = logEngineFactory.initialize(eng.getSubConfig("log"));
    kafkaLogEngine.execute(new KafkaPhysicalPlan(eng.getSubConfig("log"), kakfaPlan),
        ErrorCollector.root()).get();

    Model.RootGraphqlModel model = mapper.readValue(deployDir.resolve("server-model.json").toFile(),
        Model.RootGraphqlModel.class);
    JdbcDataSystemConnector jdbc = mapper.readValue(
        deployDir.resolve("server-config.json").toFile(), JdbcDataSystemConnector.class);
    ServerPhysicalPlan serverPhysicalPlan = new ServerPhysicalPlan(model, jdbc);
    VertxEngineFactory vertxEngineFactory = new VertxEngineFactory();
    VertxEngineFactory.VertxEngine vertxEngine = vertxEngineFactory.initialize(eng.getSubConfig("server"), vertx);
    vertxEngine.execute(serverPhysicalPlan, ErrorCollector.root()).get();

    FlinkExecutablePlan flinkPlan = mapper.readValue(deployDir.resolve("flink-plan.json").toFile(),
        FlinkExecutablePlan.class);
    FlinkStreamPhysicalPlan plan = new FlinkStreamPhysicalPlan(flinkPlan);
    LocalFlinkStreamEngineImpl localFlinkStreamEngine = new LocalFlinkStreamEngineImpl(
        new ExecutionEnvironmentFactory(Map.of()), config.getSubConfig("engines")
        .getSubConfig("stream"));
    CompletableFuture<com.datasqrl.engine.ExecutionResult> fut = localFlinkStreamEngine.execute(
        plan, ErrorCollector.root());

    return fut;
  }
}
