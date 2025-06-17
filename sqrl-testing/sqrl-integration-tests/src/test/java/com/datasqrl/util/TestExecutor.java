/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// package com.datasqrl.util;
//
// import static com.datasqrl.PlanConstants.PLAN_CONFIG;
// import static com.datasqrl.PlanConstants.PLAN_SEPARATOR;
// import static com.datasqrl.PlanConstants.PLAN_SQL;
//
// import com.datasqrl.FlinkExecutablePlan;
// import com.datasqrl.config.SqrlConfigCommons;
// import com.datasqrl.engine.ExecutionResult;
// import com.datasqrl.engine.database.relational.JDBCEngine;
// import com.datasqrl.engine.database.relational.JDBCEngineFactory;
// import com.datasqrl.engine.database.relational.JDBCPhysicalPlan;
// import com.datasqrl.engine.server.ServerPhysicalPlan;
// import com.datasqrl.engine.server.VertxEngineFactory;
// import com.datasqrl.discovery.flink.flink.ExecutionEnvironmentFactory;
// import com.datasqrl.engine.stream.flink.LocalFlinkStreamEngineImpl;
// import com.datasqrl.engine.stream.flink.plan.FlinkStreamPhysicalPlan;
// import com.datasqrl.error.ErrorCollector;
// import com.datasqrl.graphql.config.ServerConfig;
// import com.datasqrl.graphql.server.Model;
// import com.datasqrl.kafka.KafkaLogEngine;
// import com.datasqrl.kafka.KafkaLogEngineFactory;
// import com.datasqrl.kafka.KafkaPhysicalPlan;
// import com.datasqrl.kafka.NewTopic;
// import com.datasqrl.serializer.Deserializer;
// import com.datasqrl.sql.SqlDDLStatement;
// import com.fasterxml.jackson.core.type.TypeReference;
// import com.fasterxml.jackson.databind.ObjectMapper;
// import io.vertx.core.Vertx;
// import io.vertx.core.json.JsonObject;
// import java.nio.file.Files;
// import java.nio.file.Path;
// import java.util.Arrays;
// import java.util.List;
// import java.util.Map;
// import java.util.concurrent.CompletableFuture;
// import java.util.stream.Collectors;
// import lombok.SneakyThrows;
// import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
// import org.apache.calcite.rel.metadata.RelMetadataQueryBase;
// import org.apache.flink.configuration.Configuration;
// import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
// import org.apache.flink.table.api.EnvironmentSettings;
// import org.apache.flink.table.api.TableResult;
// import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
// import org.apache.flink.table.planner.plan.metadata.FlinkDefaultRelMetadataProvider;
// import org.apache.logging.log4j.util.Strings;
//
// public class TestExecutor {
//
//  private final Vertx vertx;
//
//  public TestExecutor(Vertx vertx) {
//    this.vertx = vertx;
//  }
//
//  //TODO: Migrate to pipeline deserializer
//  @SneakyThrows
//  public CompletableFuture<ExecutionResult> executePipeline(Path rootDir) {
//    return executePipeline(rootDir, false);
//  }
//
//  @SneakyThrows
//  public CompletableFuture<ExecutionResult> executePipeline(Path rootDir, boolean executeSql) {
//    ObjectMapper mapper = SqrlObjectMapper.INSTANCE;
//
//    SqrlConfig config = SqrlConfigCommons.fromFiles(ErrorCollector.root(),
//        rootDir.resolve("build").resolve("package.json"));
//
//    Path deployDir = rootDir.resolve("build").resolve("deploy");
//
//    SqrlConfig eng = config.getSubConfig("engines");
//    List<SqlDDLStatement> schema = Arrays.stream(
//            Files.readString(deployDir.resolve("database-schema.sql")).split("\n"))
//        .filter(s -> !Strings.isEmpty(s)).map(s -> (SqlDDLStatement) () -> s)
//        .collect(Collectors.toList());
//    JDBCPhysicalPlan jdbcPhysicalPlan = new JDBCPhysicalPlan(schema, Map.of());
//    JDBCEngineFactory jdbcEngineFactory = new JDBCEngineFactory();
//    JDBCEngine jdbcEngine = jdbcEngineFactory.initialize(eng.getSubConfig("database"));
//    jdbcEngine.execute(jdbcPhysicalPlan, ErrorCollector.root()).get();
//
//    List<NewTopic> kakfaPlan = mapper.readValue(deployDir.resolve("kafka-plan.json").toFile(),
//        new TypeReference<List<NewTopic>>() {
//        });
//    KafkaLogEngineFactory logEngineFactory = new KafkaLogEngineFactory();
//    KafkaLogEngine kafkaLogEngine = logEngineFactory.initialize(eng.getSubConfig("log"));
//    kafkaLogEngine.execute(new KafkaPhysicalPlan(eng.getSubConfig("log"), kakfaPlan),
//        ErrorCollector.root()).get();
//
//    Model.RootGraphqlModel model =
// mapper.readValue(deployDir.resolve("server-model.json").toFile(),
//        Model.RootGraphqlModel.class);
//    String serverConfigStr = Files.readString(deployDir.resolve("server-config.json"));
//    JsonObject serverJsonObject = new JsonObject(serverConfigStr);
//    ServerConfig serverConfig = new ServerConfig(serverJsonObject);
//
//    ServerPhysicalPlan serverPhysicalPlan = new ServerPhysicalPlan(model,serverConfig);
//    VertxEngineFactory vertxEngineFactory = new VertxEngineFactory();
//    VertxEngineFactory.VertxEngine vertxEngine =
// vertxEngineFactory.initialize(eng.getSubConfig("server"), vertx);
//    vertxEngine.execute(serverPhysicalPlan, ErrorCollector.root(), model).get();
//
//    if (!executeSql) {
//      FlinkExecutablePlan flinkPlan = mapper.readValue(
//          deployDir.resolve("flink-plan.json").toFile(),
//          FlinkExecutablePlan.class);
//      FlinkStreamPhysicalPlan plan = new FlinkStreamPhysicalPlan(flinkPlan);
//      LocalFlinkStreamEngineImpl localFlinkStreamEngine = new LocalFlinkStreamEngineImpl(
//           config.getSubConfig("engines")
//          .getSubConfig("streams"));
//      CompletableFuture<com.datasqrl.engine.ExecutionResult> fut = localFlinkStreamEngine.execute(
//          plan, ErrorCollector.root());
//      return fut;
//    } else {
//      Path planPath = deployDir.resolve(PLAN_SQL);
//      String sql = Files.readString(planPath);
//
//      Map<String, String> configMap = new
// Deserializer().mapYAMLFile(deployDir.resolve(PLAN_CONFIG), Map.class);
//
//      String[] commands = sql.split(PLAN_SEPARATOR);
//
//      Configuration sEnvConfig = Configuration.fromMap(configMap);
//      StreamExecutionEnvironment sEnv = StreamExecutionEnvironment
//          .getExecutionEnvironment(sEnvConfig);
//
//      EnvironmentSettings tEnvConfig = EnvironmentSettings.newInstance()
//          .withConfiguration(Configuration.fromMap(configMap)).build();
//      StreamTableEnvironment tEnv = StreamTableEnvironment.create(sEnv, tEnvConfig);
//
//      return CompletableFuture.supplyAsync(()->{
//        try {
//          TableResult tableResult = null;
//          for (String command : commands) {
//            String trim = command.trim();
//            if (!trim.isEmpty()) {
//              RelMetadataQueryBase.THREAD_PROVIDERS
//                  .set(JaninoRelMetadataProvider.of(FlinkDefaultRelMetadataProvider.INSTANCE()));
//
//              tableResult = tEnv.executeSql(trim);
//            }
//          }
//          tableResult.print();
//          ExecutionResult result = new ExecutionResult.Message(tableResult.getJobClient().get()
//              .getJobID().toString());
//          return result;
//        } catch (Exception e) {
//          e.printStackTrace();
//          throw e;
//        }
//      });
//    }
//
//  }
// }
