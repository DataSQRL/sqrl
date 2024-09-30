//package com.datasqrl;
//
//import com.fasterxml.jackson.databind.ObjectMapper;
//import com.fasterxml.jackson.databind.node.ObjectNode;
//import com.github.dockerjava.api.DockerClient;
//import com.github.dockerjava.api.command.BuildImageCmd;
//import com.github.dockerjava.api.command.BuildImageResultCallback;
//import com.github.dockerjava.api.model.BuildResponseItem;
//import java.io.File;
//import java.io.IOException;
//import java.nio.file.Files;
//import java.nio.file.Path;
//import org.junit.jupiter.api.BeforeAll;
//import org.junit.jupiter.api.Disabled;
//import org.junit.jupiter.api.Test;
//import org.testcontainers.DockerClientFactory;
//
//@Disabled
//public class UseCasesFullIT extends UseCasesIT {
//
//  private static String config;
//
//  @BeforeAll
//  public static void buildContainers() {
//    config = writeProfile();
//    buildVertx();
//    buildFlinkLib();
//  }
//
//  public static void buildFlinkLib() {
//    // Create a Docker client
//    DockerClient dockerClient = DockerClientFactory.instance().client();
//
//    // Define the path to the Dockerfile
//    File dockerfile = PROJECT_ROOT.toFile();
//
//    // Build and tag the Docker image
//    BuildImageCmd buildImageCmd = dockerClient.buildImageCmd(dockerfile)
//        .withTag("flink-build:latest");
//
//    // Execute the build and capture the response
//    String imageId = buildImageCmd.exec(new BuildImageResultCallback() {
//      @Override
//      public void onNext(BuildResponseItem item) {
//        super.onNext(item);
////        System.out.println(item.getStream());
//      }
//    }).awaitImageId();
//
//    // Assert or check the imageId (or perform further actions)
//    System.out.println("Built image ID: " + imageId);
//  }
//
//  public static void buildVertx() {
//    // Create a Docker client
//    DockerClient dockerClient = DockerClientFactory.instance().client();
//
//    // Define the path to the Dockerfile
//    File dockerfile = PROJECT_ROOT.resolve("sqrl-server/sqrl-server-vertx").toFile();
//
//    // Build and tag the Docker image
//    BuildImageCmd buildImageCmd = dockerClient.buildImageCmd(dockerfile)
//        .withTag("vertx-latest");
//
//    // Execute the build and capture the response
//    String imageId = buildImageCmd.exec(new BuildImageResultCallback() {
//      @Override
//      public void onNext(BuildResponseItem item) {
//        super.onNext(item);
//        System.out.println(item.getStream());
//      }
//    }).awaitImageId();
//
//    // Assert or check the imageId (or perform further actions)
//    System.out.println("Built image ID: " + imageId);
//  }
//
//  public static String writeProfile() {
//    // Create an ObjectMapper instance
//    ObjectMapper mapper = new ObjectMapper();
//
//    // Create the root JSON object
//    ObjectNode rootNode = mapper.createObjectNode();
//
//    // Create the compile object and replace the image name
//    ObjectNode compileNode = mapper.createObjectNode();
//    compileNode.put("sqrl-vertx-image", "vertx-latest:latest");
//    compileNode.put("flink-build-image", "flink-build:latest");
//    compileNode.put("sqrl-version", "0.5.5-SNAPSHOT");
//
//    // Attach the compile object to the root node
//    rootNode.set("compile", compileNode);
//    rootNode.put("version", "1");
//
//    try {
//      // Create a temporary file
//      Path tempPath = Files.createTempFile("json_temp", ".json");
//
//      // Write JSON data to the temporary file
//      mapper.writerWithDefaultPrettyPrinter().writeValue(Files.newOutputStream(tempPath), rootNode);
//      System.out.println("JSON written to: " + tempPath);
//
//      return tempPath.toAbsolutePath().toString();
//    } catch (IOException e) {
//      System.err.println("Error writing JSON to file: " + e.getMessage());
//    }
//    return null;
//  }
//
//  @Test
//  public void testBanking() {
//    execute("test","banking", "loan.sqrl", "loan.graphqls", null,
//        "-c", PROJECT_ROOT.resolve("sqrl-testing/sqrl-integration-tests/src/test/resources/usecases/banking/package.json").toString(),
//        "-c", config);
//  }
//
//  @Test
//  public void testClickstream() {
//    execute("test", "clickstream", "clickstream-teaser.sqrl", "clickstream-teaser.graphqls", null,
//        "-c", PROJECT_ROOT.resolve("sqrl-testing/sqrl-integration-tests/src/test/resources/usecases/clickstream/package.json").toString(),
//        "-c", config);
//  }
//
//  @Test
//  public void testConference() {
//    execute("test", "conference", "conference.sqrl", "conference.graphqls", null,
//        "-c", PROJECT_ROOT.resolve("sqrl-testing/sqrl-integration-tests/src/test/resources/usecases/banking/package.json").toString(),
//        "-c", config);
//  }
//
//  @Test
//  @Disabled //flakey
//  public void testSensorsMutation() {
//    execute("test", "sensors", "sensors-mutation.sqrl", null,"sensors-mutation.graphqls", "sensors-mutation",
//        "-c", PROJECT_ROOT.resolve("sqrl-testing/sqrl-integration-tests/src/test/resources/usecases/sensors/package.json").toString(),
//        "-c", config);
//  }
//
//  @Test
//  @Disabled //A compressed csv bug prevents this from completed correctly
//  public void testSensorsFull() {
//    execute("test", "sensors", "sensors-full.sqrl", null,"sensors-full",
//        "-c", PROJECT_ROOT.resolve("sqrl-testing/sqrl-integration-tests/src/test/resources/usecases/sensors/package.json").toString(),
//        "-c", config);
//  }
//
//  @Test
//  public void testSeedshopExtended() {
//    execute("test", "seedshop-tutorial", "seedshop-extended.sqrl", null, "seedshop-extended",
//        "-c", PROJECT_ROOT.resolve("sqrl-testing/sqrl-integration-tests/src/test/resources/usecases/seedshop-tutorial/package.json").toString(),
//        "-c", config);
//  }
//
//  @Test
//  @Disabled //todo
//  public void testDuckdb() {
//    compile("duckdb", "duckdb.sqrl", null);
//  }
//
////
////  @Test
////  @Disabled
////  public void compile() {
////    compile("sensors", "sensors-mutation.sqrl", "sensors-mutation.graphqls");
////  }
////
////  @Test
////  @Disabled
////  public void testCompileScript() {
////    execute(Path.of("/Users/matthias/git/data-product-data-connect-cv/src/main/datasqrl"), AssertStatusHook.INSTANCE,
////        "compile", "clinical_views.sqrl", "-c", "test_package_clinical_views.json", "--profile", "profile/");
////  }
//}
