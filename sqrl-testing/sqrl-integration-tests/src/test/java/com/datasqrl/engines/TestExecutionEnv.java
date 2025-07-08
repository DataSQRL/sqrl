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
package com.datasqrl.engines;

import static org.assertj.core.api.Assertions.fail;

import com.datasqrl.UseCaseTestParameter;
import com.datasqrl.cli.DatasqrlTest;
import com.datasqrl.config.PackageJson;
import com.datasqrl.config.SqrlConstants;
import com.datasqrl.engines.TestEngine.DuckdbTestEngine;
import com.datasqrl.engines.TestEngine.FlinkTestEngine;
import com.datasqrl.engines.TestEngine.IcebergTestEngine;
import com.datasqrl.engines.TestEngine.KafkaTestEngine;
import com.datasqrl.engines.TestEngine.PostgresLogTestEngine;
import com.datasqrl.engines.TestEngine.PostgresTestEngine;
import com.datasqrl.engines.TestEngine.SnowflakeTestEngine;
import com.datasqrl.engines.TestEngine.TestEngineVisitor;
import com.datasqrl.engines.TestEngine.TestTestEngine;
import com.datasqrl.engines.TestEngine.VertxTestEngine;
import com.datasqrl.engines.TestExecutionEnv.TestEnvContext;
import com.datasqrl.graphql.JsonEnvVarDeserializer;
import com.datasqrl.util.ConfigLoaderUtils;
import com.datasqrl.util.ResultSetPrinter;
import com.datasqrl.util.SnapshotTest.Snapshot;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.SneakyThrows;

@AllArgsConstructor
public class TestExecutionEnv implements TestEngineVisitor<Void, TestEnvContext> {

  String goal;
  PackageJson packageJson;
  Path rootDir;
  Snapshot snapshot;

  @SneakyThrows
  @Override
  public Void visit(PostgresTestEngine engine, TestEnvContext context) {
    if (hasTestEngine()) { // tested by Test goal
      return null;
    }

    if (hasServerEngine()) { // Tested by graphql queries
      return null;
    }

    // Snapshot views
    Map postgresPlan =
        new ObjectMapper()
            .readValue(rootDir.resolve("build/deploy/plan/postgres.json").toFile(), Map.class);
    List<Map<String, Object>> view = (List<Map<String, Object>>) postgresPlan.get("views");
    String url = context.env.get("JDBC_URL");
    String username = context.env.get("PGUSER");
    String password = context.env.get("PGPASSWORD");
    try (Connection conn = DriverManager.getConnection(url, username, password)) {
      for (Map statement : (List<Map>) postgresPlan.get("statements")) {
        if (statement.get("type").toString().equalsIgnoreCase("view")) {
          String viewName = (String) statement.get("name");
          ResultSet resultSet =
              conn.createStatement().executeQuery("SELECT * FROM \"%s\"".formatted(viewName));
          String string = ResultSetPrinter.toString(resultSet, (c) -> true, (c) -> true);
          snapshot.addContent(string, viewName);
        }
      }
    }

    return null;
  }

  @Override
  public Void visit(PostgresLogTestEngine engine, TestEnvContext context) {
    return null;
  }

  @Override
  public Void visit(KafkaTestEngine engine, TestEnvContext context) {
    return null;
  }

  @Override
  public Void visit(IcebergTestEngine engine, TestEnvContext context) {
    return null;
  }

  @Override
  public Void visit(DuckdbTestEngine engine, TestEnvContext context) {
    if (hasTestEngine() || hasServerEngine()) { // Tested by graphql queries
      return null;
    }

    return null;
  }

  @SneakyThrows
  @Override
  public Void visit(VertxTestEngine engine, TestEnvContext context) {
    // Go through and execute each query (if not test goal)
    if (hasTestEngine()) {
      return null;
    }

    if (context.getParam().getTestPath() != null) {
      Path testPath = context.rootDir.resolve(context.getParam().getTestPath());
      try (DirectoryStream<Path> directoryStream =
          Files.newDirectoryStream(testPath, "*.graphql")) {
        List<Path> paths = new ArrayList<>();
        directoryStream.forEach(paths::add);

        // Sort the paths by filename
        paths.sort(Comparator.comparing(p -> p.getFileName().toString()));

        for (Path path : paths) {
          String query = Files.readString(path);
          String s = executeQuery(query);
          snapshot.addContent(s, path.getFileName().toString());
        }
      }
    }

    return null;
  }

  @SneakyThrows
  private String executeQuery(String query) {
    HttpClient client = HttpClient.newHttpClient();

    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(URI.create("http://localhost:8888/graphql"))
            .header("Content-Type", "application/graphql")
            .POST(HttpRequest.BodyPublishers.ofString(query))
            .build();

    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

    if (response.statusCode() != 200) {
      throw new RuntimeException(
          "Failed to post GraphQL query: " + response.body() + " test case: " + goal);
    }

    return response.body();
  }

  @SneakyThrows
  @Override
  public Void visit(SnowflakeTestEngine engine, TestEnvContext context) {
    if (hasTestEngine()) { // tested by Test goal
      return null;
    }

    // Install the snowflake schema

    ObjectMapper mapper = new ObjectMapper();
    SimpleModule module = new SimpleModule();
    module.addDeserializer(String.class, new JsonEnvVarDeserializer(context.getEnv()));
    mapper.registerModule(module);

    Path schema = context.getRootDir().resolve("build/plan/iceberg.json");
    Map map = mapper.readValue(schema.toFile(), Map.class);
    Map<String, List<Map<String, String>>> snowflake =
        (Map<String, List<Map<String, String>>>) ((Map) map.get("engines")).get("snowflake");

    String url =
        packageJson
            .getEngines()
            .getEngineConfig("snowflake")
            .get()
            .getSetting("url", Optional.empty());

    try (Connection connection = DriverManager.getConnection(url)) {
      for (Map<String, String> ddls : snowflake.get("ddl")) {
        boolean execute = connection.createStatement().execute(ddls.get("sql"));
        //        if (!execute) fail("Could not execute query:" + ddls.get("sql"));
      }
      for (Map<String, String> ddls : snowflake.get("views")) {
        boolean execute = connection.createStatement().execute(ddls.get("sql"));
        //        if (!execute) fail("Could not execute query:" + ddls.get("sql"));
      }

      for (Map<String, String> ddls : snowflake.get("views")) {
        ResultSet execute =
            connection
                .createStatement()
                .executeQuery("SELECT * FROM %s".formatted(ddls.get("name")));
        String string = ResultSetPrinter.toString(execute, (c) -> true, (c) -> true);
        snapshot.addContent(string, ddls.get("name"));
      }
    }

    return null;
  }

  @Override
  public Void visit(FlinkTestEngine engine, TestEnvContext context) {
    // todo test for flink-only use cases

    return null;
  }

  @SneakyThrows
  @Override
  public Void visit(TestTestEngine engine, TestEnvContext context) {
    var planDir =
        context
            .rootDir
            .resolve(SqrlConstants.BUILD_DIR_NAME)
            .resolve(SqrlConstants.DEPLOY_DIR_NAME)
            .resolve(SqrlConstants.PLAN_DIR);
    var flinkConfig = ConfigLoaderUtils.loadFlinkConfig(planDir);
    var test = new DatasqrlTest(context.rootDir, planDir, packageJson, flinkConfig, context.env);
    try {
      var run = test.run();
      if (run != 0) {
        fail(
            "Test runner returned error code while running test case '%s'. Check above for failed snapshot tests (in red) or exceptions"
                .formatted(goal));
      }
    } catch (Exception e) {
      fail("Test runner threw exception while running test case '%s'".formatted(goal), e);
    }
    return null;
  }

  private boolean hasTestEngine() {
    return packageJson.getEnabledEngines().contains("test");
  }

  private boolean hasServerEngine() {
    return packageJson.getEnabledEngines().contains("vertx");
  }

  @Builder
  @Getter
  public static class TestEnvContext {
    Path rootDir;
    Map<String, String> env;
    UseCaseTestParameter param;
  }
}
