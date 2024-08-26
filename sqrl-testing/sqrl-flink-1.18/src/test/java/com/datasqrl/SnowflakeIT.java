package com.datasqrl;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import lombok.SneakyThrows;
import org.apache.directory.api.util.Strings;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class SnowflakeIT {

  static DatasqrlRun datasqrlRun;


  @BeforeAll
  static void beforeAll() {
    datasqrlRun = new DatasqrlRun();
    datasqrlRun.startKafkaCluster();
  }

  @SneakyThrows
  @Test
  @Disabled
  public void test() {
    Path directoryPath = Path.of(
        "/Users/henneberger/sqrl/sqrl-testing/sqrl-integration-tests/src/test/resources/usecases/snowflake");
    SqrlCompiler compiler = new SqrlCompiler();
    compiler.execute(directoryPath, "compile", "--profile", "/Users/henneberger/sqrl/profiles/default");

    datasqrlRun.setPath(directoryPath.resolve("build").resolve("plan"));

    datasqrlRun.run(false);

    Thread.sleep(1000);
    Path schema = directoryPath.resolve("build/deploy/snowflake/database-schema.sql");
    String[] statements = Files.readString(schema).split(";");
    for (String statement : statements) {
      statement = Strings.trim(statement);

      if (Strings.isEmpty(statement)) continue;
      try (Connection connection = DriverManager.getConnection(
          datasqrlRun.getSnowflakeUrl().get())) {
        connection.createStatement().execute(statement);
      }
    }

    String graphqlEndpoint = "http://localhost:8888/graphql";
    String query = "query {\n"
        + "  SnowflakeQuery(customerId:1) {\n"
        + "    customer_id\n"
        + "    amount\n"
        + "  }\n"
        + "  MyIcebergTable(limit:1) {\n"
        + "    customer_id\n"
        + "  }\n"
        + "}";

    String response = executeQuery(graphqlEndpoint, query);


    assertEquals(response, "{\"data\":{\"SnowflakeQuery\":[{\"customer_id\":1.0,\"amount\":706721.71},{\"customer_id\":1.0,\"amount\":706721.71},{\"customer_id\":1.0,\"amount\":706721.71},{\"customer_id\":1.0,\"amount\":42079.16},{\"customer_id\":1.0,\"amount\":42079.16},{\"customer_id\":1.0,\"amount\":42079.16},{\"customer_id\":1.0,\"amount\":463121.85},{\"customer_id\":1.0,\"amount\":463121.85},{\"customer_id\":1.0,\"amount\":463121.85},{\"customer_id\":1.0,\"amount\":290005.57},{\"customer_id\":1.0,\"amount\":290005.57},{\"customer_id\":1.0,\"amount\":290005.57},{\"customer_id\":1.0,\"amount\":253812.11},{\"customer_id\":1.0,\"amount\":253812.11},{\"customer_id\":1.0,\"amount\":253812.11},{\"customer_id\":1.0,\"amount\":29341.72},{\"customer_id\":1.0,\"amount\":29341.72},{\"customer_id\":1.0,\"amount\":29341.72},{\"customer_id\":1.0,\"amount\":243720.97},{\"customer_id\":1.0,\"amount\":243720.97},{\"customer_id\":1.0,\"amount\":243720.97},{\"customer_id\":1.0,\"amount\":16778.48},{\"customer_id\":1.0,\"amount\":16778.48},{\"customer_id\":1.0,\"amount\":16778.48},{\"customer_id\":1.0,\"amount\":255874.79},{\"customer_id\":1.0,\"amount\":255874.79},{\"customer_id\":1.0,\"amount\":255874.79},{\"customer_id\":1.0,\"amount\":605384.78},{\"customer_id\":1.0,\"amount\":605384.78},{\"customer_id\":1.0,\"amount\":605384.78},{\"customer_id\":1.0,\"amount\":379228.36},{\"customer_id\":1.0,\"amount\":379228.36},{\"customer_id\":1.0,\"amount\":379228.36},{\"customer_id\":1.0,\"amount\":8355.88},{\"customer_id\":1.0,\"amount\":8355.88},{\"customer_id\":1.0,\"amount\":8355.88},{\"customer_id\":1.0,\"amount\":39127.77},{\"customer_id\":1.0,\"amount\":39127.77},{\"customer_id\":1.0,\"amount\":39127.77},{\"customer_id\":1.0,\"amount\":252694.58},{\"customer_id\":1.0,\"amount\":252694.58},{\"customer_id\":1.0,\"amount\":252694.58},{\"customer_id\":1.0,\"amount\":214574.98},{\"customer_id\":1.0,\"amount\":214574.98},{\"customer_id\":1.0,\"amount\":214574.98}],\"MyIcebergTable\":[{\"customer_id\":4.0}]}}\n");
  }

  private static String executeQuery(String endpoint, String query) {
    HttpClient client = HttpClient.newHttpClient();
    HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create(endpoint))
        .header("Content-Type", "application/graphql")
        .POST(HttpRequest.BodyPublishers.ofString(query))
        .build();

    try {
      HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
      return response.body();
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
      return null;
    }
  }
}
