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
// package com.datasqrl.flink;
//
// import static org.junit.jupiter.api.Assertions.assertEquals;
//
// import com.datasqrl.cli.DatasqrlRun;
// import java.io.IOException;
// import java.net.URI;
// import java.net.http.HttpClient;
// import java.net.http.HttpRequest;
// import java.net.http.HttpResponse;
// import java.nio.file.Path;
// import java.nio.file.Paths;
// import lombok.SneakyThrows;
// import org.junit.jupiter.api.BeforeAll;
// import org.junit.jupiter.api.Disabled;
// import org.junit.jupiter.api.Test;
//
// public class LogEngineIT {
//  static DatasqrlRun datasqrlRun;
//
//  @BeforeAll
//  static void beforeAll() {
//    datasqrlRun = new DatasqrlRun();
//  }
//
//  @SneakyThrows
//  @Test
//  @Disabled
//  //Must have SNOWFLAKE_PASSWORD in env when running test
//  public void test() {
//    Path projectRoot = getProjectRootPath();
//    Path testRoot =
// projectRoot.resolve("sqrl-testing/sqrl-testing-integration/src/test/resources/usecases/postgres-log");
//    Path profilePath = projectRoot.resolve("profiles/default");
//    SqrlCompiler compiler = new SqrlCompiler();
//    compiler.execute(testRoot, "compile", "--profile", profilePath.toString());
//
//    datasqrlRun.setPath(testRoot.resolve("build").resolve("plan"));
//
//    datasqrlRun.run(false);
//
//    Thread.sleep(2000);
//
//    String graphqlEndpoint = "http://localhost:8888/graphql";
//    String mut = "mutation {\n"
//        + "  Event(event:{\n"
//        + "    ID:1,\n"
//        + "    event_time:\"2024-11-01T20:44:39Z\",\n"
//        + "    SOME_VALUE:\"3\"\n"
//        + "  }) {\n"
//        + "    ID\n"
//        + "  }\n"
//        + "}";
//
//    String response = executeQuery(graphqlEndpoint, mut);
//
//    Thread.sleep(2000);
//    String query = "{\n"
//       + "  MyEvent {\n"
//       + "    ID\n"
//       + "  }\n"
//       + "}";
//
//    String resp = executeQuery(graphqlEndpoint, query);
//
//    assertEquals("{\"data\":{\"MyEvent\":[{\"ID\":1.0}]}}", resp);
//  }
//
//  //todo move to lib
//  private static String executeQuery(String endpoint, String query) {
//    HttpClient client = HttpClient.newHttpClient();
//    HttpRequest request = HttpRequest.newBuilder()
//        .uri(URI.create(endpoint))
//        .header("Content-Type", "application/graphql")
//        .POST(HttpRequest.BodyPublishers.ofString(query))
//        .build();
//
//    try {
//      HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
//      return response.body();
//    } catch (IOException | InterruptedException e) {
//      e.printStackTrace();
//      return null;
//    }
//  }
//
//  //todo move to lib
//  private Path getProjectRootPath() {
//    Path path = Paths.get(".").toAbsolutePath().normalize();
//    Path rootPath = null;
//    while (path != null) {
//      if (path.resolve("pom.xml").toFile().exists()) {
//        rootPath = path;
//      }
//      path = path.getParent();
//    }
//    return rootPath;
//  }
// }
