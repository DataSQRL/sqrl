/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.flink;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.datasqrl.FlinkExecutablePlan;
import com.datasqrl.FlinkExecutablePlan.FlinkBase;
import com.datasqrl.cmd.AssertStatusHook;
import com.datasqrl.cmd.RootCommand;
import com.datasqrl.config.SqrlConfig;
import com.datasqrl.config.SqrlConfigCommons;
import com.datasqrl.engine.ExecutionResult;
import com.datasqrl.engine.database.relational.JDBCEngine;
import com.datasqrl.engine.database.relational.JDBCEngineFactory;
import com.datasqrl.engine.database.relational.JDBCPhysicalPlan;
import com.datasqrl.engine.database.relational.ddl.SqlDDLStatement;
import com.datasqrl.engine.server.ServerPhysicalPlan;
import com.datasqrl.engine.server.VertxEngineFactory;
import com.datasqrl.engine.server.VertxEngineFactory.VertxEngine;
import com.datasqrl.engine.stream.flink.ExecutionEnvironmentFactory;
import com.datasqrl.engine.stream.flink.LocalFlinkStreamEngineImpl;
import com.datasqrl.engine.stream.flink.plan.FlinkStreamPhysicalPlan;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.graphql.server.Model.RootGraphqlModel;
import com.datasqrl.io.impl.jdbc.JdbcDataSystemConnector;
import com.datasqrl.kafka.KafkaLogEngine;
import com.datasqrl.kafka.KafkaLogEngineFactory;
import com.datasqrl.kafka.KafkaPhysicalPlan;
import com.datasqrl.packager.PackagerConfig;
import com.datasqrl.serializer.Deserializer;
import com.datasqrl.service.PackagerUtil;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.logging.log4j.util.Strings;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
public class FlinkMutationSubscriptionTest {

  ObjectMapper m = new Deserializer().getJsonMapper();

  @Container
  KafkaContainer kafka = new KafkaContainer(
      DockerImageName.parse("confluentinc/cp-kafka:7.4.0"));
  @Container
  private PostgreSQLContainer testDatabase =
      new PostgreSQLContainer(DockerImageName.parse("postgres:14.2"))
          .withDatabaseName("foo")
          .withUsername("foo")
          .withPassword("secret")
          .withDatabaseName("datasqrl");

  @SneakyThrows
  @Test
  public void runSpecificTest() {
    System.out.println(kafka.getBootstrapServers());
    Path rootDir = Path.of("../../sqrl-examples/mutations");
    Path script = rootDir.resolve("script.sqrl");
    Path graphql = rootDir.resolve("schema.graphqls");

    Path override = createPackageOverride(kafka, testDatabase);
    Path defaultPackage = createDefaultPackage(rootDir, script, graphql);

    //1. Call CLI on static example
    picocli.CommandLine rootCommand = new RootCommand(rootDir, AssertStatusHook.INSTANCE).getCmd();

    int code = rootCommand.execute("compile",
        script.toString(), graphql.toString(),
        "-c", defaultPackage.toAbsolutePath().toString(), "-c", override.toAbsolutePath().toString());
    assertEquals(0, code, "Non-zero exit code");

    //2. Get deploy folder and reconstruct assets for engine execution
    SqrlConfig config = SqrlConfigCommons.fromFiles(ErrorCollector.root(),
        rootDir.resolve("build").resolve("package.json"));

    Path deployDir = rootDir.resolve("build").resolve("deploy");

    //This is a bit manual, needs abstracting
    //

    SqrlConfig eng = config.getSubConfig("engines");
    List<SqlDDLStatement> schema = Arrays.stream(Files.readString(deployDir.resolve("database-schema.sql")).split("\n"))
        .filter(s-> Strings.isEmpty(s))
        .map(s-> (SqlDDLStatement) () -> s)
        .collect(Collectors.toList());
    JDBCPhysicalPlan jdbcPhysicalPlan = new JDBCPhysicalPlan(schema, Map.of());
    JDBCEngineFactory jdbcEngineFactory = new JDBCEngineFactory();
    JDBCEngine jdbcEngine = jdbcEngineFactory.initialize(eng.getSubConfig("database"));
    jdbcEngine.execute(jdbcPhysicalPlan, ErrorCollector.root())
        .get();

    List<NewTopic> kakfaPlan = m.readValue(deployDir.resolve("kafka-plan.json").toFile(),
        new TypeReference<List<NewTopic>>() {});
    KafkaLogEngineFactory logEngineFactory = new KafkaLogEngineFactory();
    KafkaLogEngine kafkaLogEngine = logEngineFactory.initialize(eng.getSubConfig("log"));
    kafkaLogEngine.execute(new KafkaPhysicalPlan(eng.getSubConfig("log"), kakfaPlan), ErrorCollector.root())
        .get();

    RootGraphqlModel model = m.readValue(deployDir.resolve("server-model.json").toFile(), RootGraphqlModel.class);
    JdbcDataSystemConnector jdbc = m.readValue(deployDir.resolve("server-config.json").toFile(), JdbcDataSystemConnector.class);
    ServerPhysicalPlan serverPhysicalPlan = new ServerPhysicalPlan(model, jdbc);
    VertxEngineFactory vertxEngineFactory = new VertxEngineFactory();
    VertxEngine vertxEngine = vertxEngineFactory.initialize(eng.getSubConfig("server"));
    CompletableFuture<ExecutionResult> vertxFut = vertxEngine.execute(serverPhysicalPlan, ErrorCollector.root());

    FlinkBase flinkBase = m.readValue(deployDir.resolve("flink-plan.json").toFile(), FlinkBase.class);
    FlinkStreamPhysicalPlan plan = new FlinkStreamPhysicalPlan(
        new FlinkExecutablePlan(flinkBase));
    LocalFlinkStreamEngineImpl localFlinkStreamEngine = new LocalFlinkStreamEngineImpl(
        new ExecutionEnvironmentFactory(Map.of()));
    CompletableFuture<com.datasqrl.engine.ExecutionResult> fut
        = localFlinkStreamEngine.execute(plan, ErrorCollector.root());

    //Will never end, needs canceling
    fut.get();
    vertxFut.get();
  }

  @SneakyThrows
  private Path createDefaultPackage(Path rootDir, Path script, Path graphql) {
    SqrlConfig config = PackagerUtil.createDockerConfig(null, null, ErrorCollector.root());
    PackagerConfig.PackagerConfigBuilder pkgBuilder =
        PackagerConfig.builder()
            .rootDir(rootDir)
            .config(config)
            .mainScript(script)
            .graphQLSchemaFile(graphql);
    PackagerConfig packagerConfig = pkgBuilder.build();

    Path defaultPackage = Files.createTempFile("pkJson", ".json");
    config.toFile(defaultPackage, true);
    return defaultPackage;
  }

  private Path createPackageOverride(KafkaContainer kafka, PostgreSQLContainer testDatabase) {
    Map overrideConfig = Map.of("engines",
        Map.of("log", Map.of("connector", Map.of("bootstrap.servers",
                "localhost:"+ kafka.getBootstrapServers().split(":")[2])),
            "database", Map.of(
                "database", testDatabase.getDatabaseName(),
                "dialect", "postgres",
                "driver", testDatabase.getDriverClassName(),
                "port", testDatabase.getMappedPort(5432),
                "host", testDatabase.getHost(),
                "user", testDatabase.getUsername(),
                "password", testDatabase.getPassword(),
                "url", testDatabase.getJdbcUrl()
            )));
    return writeJson(overrideConfig);
  }

  @SneakyThrows
  private Path writeJson(Map overrideConfig) {
    Path tmp = Files.createTempFile("pkjson", ".json");
    m.writeValue(tmp.toFile(), overrideConfig);
    return tmp;
  }
}
