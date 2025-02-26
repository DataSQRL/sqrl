// package com.datasqrl;
//
// import static org.junit.jupiter.api.Assertions.assertEquals;
//
// import java.io.IOException;
// import java.net.URI;
// import java.net.http.HttpClient;
// import java.net.http.HttpRequest;
// import java.net.http.HttpResponse;
// import java.nio.file.Files;
// import java.nio.file.Path;
// import java.nio.file.Paths;
// import java.sql.Connection;
// import java.sql.DriverManager;
// import lombok.SneakyThrows;
// import org.apache.directory.api.util.Strings;
// import org.junit.jupiter.api.BeforeAll;
// import org.junit.jupiter.api.BeforeEach;
// import org.junit.jupiter.api.Disabled;
// import org.junit.jupiter.api.Test;
// import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
// import software.amazon.awssdk.regions.Region;
// import software.amazon.awssdk.services.glue.GlueClient;
// import software.amazon.awssdk.services.glue.model.DeleteTableRequest;
// import software.amazon.awssdk.services.glue.model.GetTablesRequest;
// import software.amazon.awssdk.services.glue.model.GetTablesResponse;
// import software.amazon.awssdk.services.glue.model.GlueException;
// import software.amazon.awssdk.services.glue.model.Table;
// import software.amazon.awssdk.services.s3.S3Client;
// import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
// import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
// import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
// import software.amazon.awssdk.services.s3.model.S3Object;
//
// public class LogEngineIT {
//  static DatasqrlRun datasqrlRun;
//
//  @BeforeAll
//  static void beforeAll() {
//    datasqrlRun = new DatasqrlRun();
//    datasqrlRun.startKafkaCluster();
//  }
//
//  @SneakyThrows
//  @Test
//  @Disabled
//  //Must have SNOWFLAKE_PASSWORD in env when running test
//  public void test() {
//    Path projectRoot = getProjectRootPath();
//    Path testRoot =
// projectRoot.resolve("sqrl-testing/sqrl-integration-tests/src/test/resources/usecases/postgres-log");
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
