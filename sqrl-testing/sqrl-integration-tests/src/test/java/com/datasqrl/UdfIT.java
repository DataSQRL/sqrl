package com.datasqrl;

import static org.junit.jupiter.api.Assertions.fail;

import com.datasqrl.cmd.RootCommand;
import com.datasqrl.cmd.StatusHook;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.util.Map;
import lombok.SneakyThrows;
import org.apache.flink.table.api.TableResult;
import org.gradle.tooling.GradleConnector;
import org.gradle.tooling.ProjectConnection;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.redpanda.RedpandaContainer;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
public class UdfIT {
  @Container
  private PostgreSQLContainer testDatabase =
      new PostgreSQLContainer(DockerImageName.parse("ankane/pgvector:v0.5.0")
          .asCompatibleSubstituteFor("postgres"))
          .withDatabaseName("foo")
          .withUsername("foo")
          .withPassword("secret")
          .withDatabaseName("datasqrl");

  @Container
  RedpandaContainer container =
      new RedpandaContainer("docker.redpanda.com/redpandadata/redpanda:v23.1.2");

  private static final String GRAPHQL_ENDPOINT = "http://localhost:8888/graphql";

  //These tests don't use any external sources/sink. Only create table statements and export to log.
  @SneakyThrows
  @Test
  public void test() {
    buildUdf();
    Path path = Path.of(
        "/Users/henneberger/sqrl/sqrl-testing/sqrl-integration-tests/src/test/resources/udf");

    execute(path, StatusHook.NONE,"compile", "myudf.sqrl");
    Map<String, String> env = Map.of(
        "JDBC_URL", testDatabase.getJdbcUrl(),
        "PGHOST", testDatabase.getHost(),
        "PGUSER", testDatabase.getUsername(),
        "JDBC_USERNAME", testDatabase.getUsername(),
        "JDBC_PASSWORD", testDatabase.getPassword(),
        "PGPORT", testDatabase.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT).toString(),
        "PGPASSWORD", testDatabase.getPassword(),
        "PGDATABASE", testDatabase.getDatabaseName(),
        "UDF_JAR_DIR",
        "/Users/henneberger/sqrl/sqrl-testing/sqrl-integration-tests/src/test/resources/udf/build/deploy/flink/lib",
        "PROPERTIES_BOOTSTRAP_SERVERS", container.getBootstrapServers()
    );
    DatasqrlRun run = new DatasqrlRun(path.resolve("build").resolve("plan"),
       env);
    TableResult run1 = run.run(false);

    int count = 10;
//    postGraphQLMutations(count);
    getGraphqlQuery();
    run.stop();
  }

  @SneakyThrows
  private void postGraphQLMutations(int count) {
    HttpClient client = HttpClient.newHttpClient();

    for (int i = 1; i <= count; i++) {
      String mutation = String.format(
          "{\"query\":\"mutation MyTable($event: MyTableInput!) { MyTable(event: $event) { id } }\",\"variables\":{\"event\":{\"id\":%d}}}",
          i);

      HttpRequest request = HttpRequest.newBuilder()
          .uri(URI.create(GRAPHQL_ENDPOINT))
          .header("Content-Type", "application/json")
          .POST(HttpRequest.BodyPublishers.ofString(mutation))
          .build();

      HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

      if (response.statusCode() != 200) {
        fail("Failed to post GraphQL mutation: " + response.body());
      }

      System.out.println("Posted GraphQL mutation with id " + i + ": " + response.body());
    }
  }

  @SneakyThrows
  private void getGraphqlQuery() {
    HttpClient client = HttpClient.newHttpClient();
    String query = "query {\n"
        + "  MyTable {\n"
        + "    val\n"
        + "  }\n"
        + "}";

    long startTime = System.currentTimeMillis();
    long timeout = 10000; // 10 seconds timeout
    int expectedRecordCount = 10;
    int recordCount = 0;

    while (System.currentTimeMillis() - startTime < timeout) {
      HttpRequest request = HttpRequest.newBuilder()
          .uri(URI.create(GRAPHQL_ENDPOINT))
          .header("Content-Type", "application/graphql")
          .POST(HttpRequest.BodyPublishers.ofString(query))
          .build();

      HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

      if (response.statusCode() != 200) {
        fail("Failed to post GraphQL query: " + response.body());
      }

      // Parse the response body as JSON
      ObjectMapper mapper = new ObjectMapper();
      JsonNode jsonResponse = mapper.readTree(response.body());

      // Navigate through the JSON to get the 'MyTable' data
      JsonNode myTableData = jsonResponse.path("data").path("MyTable");

      if (!myTableData.isArray()) {
        continue;
      }

      // Check the number of records
      recordCount = myTableData.size();
      if (recordCount >= expectedRecordCount) {
        System.out.println("Successfully retrieved " + recordCount + " records.");
        break;  // Exit loop once 10 records are found
      }

      // Wait for 1 second before the next request
      Thread.sleep(1000);
    }

    if (recordCount < expectedRecordCount) {
      fail("Failed to retrieve 10 records within the timeout period. Only got " + recordCount);
    }
  }

  public void buildUdf() {
    File projectDir = new File("/Users/henneberger/sqrl/sqrl-testing/sqrl-integration-tests/src/test/resources/udf/myjavafunction"); // Update with your project path
    try {
      // Create a process builder to run 'gradle build'
      ProcessBuilder processBuilder = new ProcessBuilder("gradle", "shadowJar");

      // Set the working directory to the project root
      processBuilder.directory(projectDir);  // Update with your project path

      // Start the process
      Process process = processBuilder.start();

      // Capture the output
      BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
      String line;
      while ((line = reader.readLine()) != null) {
        System.out.println(line);
      }

      // Wait for the process to finish
      int exitCode = process.waitFor();
      System.out.println("\nBuild finished with exit code: " + exitCode);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
  public static int execute(Path rootDir, StatusHook hook, String... args) {
    RootCommand rootCommand = new RootCommand(rootDir, hook);
    int exitCode = rootCommand.getCmd().execute(args) + (hook.isSuccess() ? 0 : 1);
    if (exitCode != 0) {
      fail();
    }
    return exitCode;
  }

}
