package com.datasqrl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import lombok.SneakyThrows;
import org.apache.directory.api.util.Strings;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.DeleteTableRequest;
import software.amazon.awssdk.services.glue.model.GetTablesRequest;
import software.amazon.awssdk.services.glue.model.GetTablesResponse;
import software.amazon.awssdk.services.glue.model.GlueException;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

public class SnowflakeIT {
  String databaseName = "mydatabase";
  Region region = Region.US_EAST_1; // Adjust the region as needed

  static DatasqrlRun datasqrlRun;


  @BeforeAll
  static void beforeAll() {
    datasqrlRun = new DatasqrlRun();
//    datasqrlRun.startKafkaCluster();
  }

  @SneakyThrows
  @Test
  @Disabled
  //Must have SNOWFLAKE_PASSWORD in env when running test
  public void test() {
    Path projectRoot = getProjectRootPath();
    Path testRoot = projectRoot.resolve("sqrl-testing/sqrl-integration-tests/src/test/resources/usecases/snowflake");
    Path profilePath = projectRoot.resolve("profiles/default");
    SqrlCompiler compiler = new SqrlCompiler();
    compiler.execute(testRoot, "compile", "--profile", profilePath.toString());

    datasqrlRun.setPath(testRoot.resolve("build").resolve("plan"));

    datasqrlRun.run(false);

    int count = 5;
    while (!hasTable() && count != 0) {
      System.out.println("Still waiting for glue table....");
      Thread.sleep(10000);
      count -= 1;
    }
    System.out.println("Waiting for job to finish");
    Thread.sleep(100000);

    Path schema = testRoot.resolve("build/deploy/snowflake/snowflake-schema.sql");
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
        + "}";

    String response = executeQuery(graphqlEndpoint, query);

    assertEquals("{\"data\":{\"SnowflakeQuery\":[{\"customer_id\":1.0,\"amount\":706721.71},{\"customer_id\":1.0,\"amount\":42079.16},{\"customer_id\":1.0,\"amount\":463121.85},{\"customer_id\":1.0,\"amount\":290005.57},{\"customer_id\":1.0,\"amount\":253812.11},{\"customer_id\":1.0,\"amount\":29341.72},{\"customer_id\":1.0,\"amount\":243720.97},{\"customer_id\":1.0,\"amount\":16778.48},{\"customer_id\":1.0,\"amount\":255874.79},{\"customer_id\":1.0,\"amount\":605384.78},{\"customer_id\":1.0,\"amount\":379228.36},{\"customer_id\":1.0,\"amount\":8355.88},{\"customer_id\":1.0,\"amount\":39127.77},{\"customer_id\":1.0,\"amount\":252694.58},{\"customer_id\":1.0,\"amount\":214574.98}]}}", response);
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

  private Path getProjectRootPath() {
    Path path = Paths.get(".").toAbsolutePath().normalize();
    Path rootPath = null;
    while (path != null) {
      if (path.resolve("pom.xml").toFile().exists()) {
        rootPath = path;
      }
      path = path.getParent();
    }
    return rootPath;
  }

  public boolean hasTable() {
    GetTablesRequest getTablesRequest = GetTablesRequest.builder()
        .databaseName(databaseName)
        .build();

    GlueClient glueClient = GlueClient.builder()
        .region(region)
        .credentialsProvider(ProfileCredentialsProvider.create())
        .build();

    GetTablesResponse getTablesResponse = glueClient.getTables(getTablesRequest);

    for (Table table : getTablesResponse.tableList()) {
      return true;
    }

    return false;
  }

  @BeforeEach
//  @AfterEach
  public void delete() {
    eraseS3();
    eraseGlue();
  }

  public void eraseGlue() {

    GlueClient glueClient = GlueClient.builder()
        .region(region)
        .credentialsProvider(ProfileCredentialsProvider.create())
        .build();

    try {
      // List all tables in the database
      GetTablesRequest getTablesRequest = GetTablesRequest.builder()
          .databaseName(databaseName)
          .build();

      GetTablesResponse getTablesResponse = glueClient.getTables(getTablesRequest);

      // Delete each table
      for (Table table : getTablesResponse.tableList()) {
        DeleteTableRequest deleteTableRequest = DeleteTableRequest.builder()
            .databaseName(databaseName)
            .name(table.name())
            .build();

        glueClient.deleteTable(deleteTableRequest);
        System.out.println("Deleted table: " + table.name());
      }

      System.out.println("All tables deleted from the Glue database: " + databaseName);

    } catch (GlueException e) {
      System.err.println("Failed to delete tables: " + e.awsErrorDetails().errorMessage());
      e.printStackTrace();
    } finally {
      glueClient.close();
    }
  }

  public void eraseS3() {
    String bucketName = "daniel-iceberg-table-test";

    Region region = Region.US_EAST_1; // Change the region if necessary
    S3Client s3 = S3Client.builder()
        .region(region)
        .credentialsProvider(ProfileCredentialsProvider.create())
        .build();

    // List all objects in the bucket
    ListObjectsV2Request listObjectsReq = ListObjectsV2Request.builder()
        .bucket(bucketName)
        .build();

    ListObjectsV2Response listObjectsRes;

    do {
      listObjectsRes = s3.listObjectsV2(listObjectsReq);

      for (S3Object s3Object : listObjectsRes.contents()) {
        // Delete each object
        DeleteObjectRequest deleteObjectReq = DeleteObjectRequest.builder()
            .bucket(bucketName)
            .key(s3Object.key())
            .build();

        s3.deleteObject(deleteObjectReq);
        System.out.println("Deleted: " + s3Object.key());
      }

      // If there are more objects to delete, set the continuation token
      listObjectsReq = listObjectsReq.toBuilder()
          .continuationToken(listObjectsRes.nextContinuationToken())
          .build();

    } while (listObjectsRes.isTruncated());

    s3.close();
    System.out.println("All files deleted from the bucket: " + bucketName);


  }
}
