//package com.datasqrl;
//
//import static org.junit.jupiter.api.Assertions.assertEquals;
//import static org.junit.jupiter.api.Assertions.fail;
//
//import com.datasqrl.cmd.AssertStatusHook;
//import com.datasqrl.cmd.RootCommand;
//import com.datasqrl.cmd.StatusHook;
//import com.datasqrl.config.PackageJson;
//import com.datasqrl.config.SqrlConfigCommons;
//import com.datasqrl.engines.TestContainersForTestGoal;
//import com.datasqrl.engines.TestContainersForTestGoal.TestContainerHook;
//import com.datasqrl.engines.TestEngine.EngineFactory;
//import com.datasqrl.engines.TestEngines;
//import com.datasqrl.engines.TestExecutionEnv;
//import com.datasqrl.engines.TestExecutionEnv.TestEnvContext;
//import com.datasqrl.error.ErrorCollector;
//import java.io.IOException;
//import java.net.URI;
//import java.net.http.HttpClient;
//import java.net.http.HttpRequest;
//import java.net.http.HttpResponse;
//import java.nio.file.Files;
//import java.nio.file.Path;
//import java.nio.file.Paths;
//import java.sql.Connection;
//import java.sql.DriverManager;
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import lombok.SneakyThrows;
//import lombok.extern.slf4j.Slf4j;
//import org.apache.flink.calcite.shaded.com.google.common.base.Strings;
//import org.junit.jupiter.api.AfterEach;
//import org.junit.jupiter.api.Disabled;
//import org.junit.jupiter.api.Test;
//import org.testcontainers.junit.jupiter.Testcontainers;
//import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
//import software.amazon.awssdk.regions.Region;
//import software.amazon.awssdk.services.glue.GlueClient;
//import software.amazon.awssdk.services.glue.model.DeleteTableRequest;
//import software.amazon.awssdk.services.glue.model.GetTablesRequest;
//import software.amazon.awssdk.services.glue.model.GetTablesResponse;
//import software.amazon.awssdk.services.glue.model.GlueException;
//import software.amazon.awssdk.services.glue.model.Table;
//import software.amazon.awssdk.services.s3.S3Client;
//import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
//import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
//import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
//import software.amazon.awssdk.services.s3.model.S3Object;
//
///**
// * Tests some use cases in the test/resources/usecases folder using the `test` command.
// */
//@Testcontainers
//@Slf4j
////@ExtendWith(MiniClusterExtension.class)
//public class UseCasesNewIT {
//  String databaseName = "mydatabase";
//
//  protected static final Path PROJECT_ROOT = getProjectRoot();
//  public static final Path RESOURCES = Paths.get("src/test/resources");
//
//  private TestContainerHook containerHook;
//
//  @AfterEach
//  public void tearDown() throws Exception {
//    if (containerHook != null) {
//      containerHook.stop();
//    }
//  }
//
//  @Test //Done
//  public void testRepository() {
//    execute("repository", "repo.sqrl", "repo.graphqls");
//  }
//
//  @Test
//  public void testLoggingToKafka() {
//    execute("logging/it-kafka", "logging-kafka.sqrl", "logging-kafka.graphqls");
//  }
//
//  @Test
//  public void testBanking() {
//    execute("test","banking", "loan.sqrl", "loan.graphqls", null,
//        "-c", PROJECT_ROOT.resolve("sqrl-testing/sqrl-integration-tests/src/test/resources/usecases/banking/package.json").toString());
//  }
//
//  @Test
//  public void testClickstream() {
//    execute("test", "clickstream", "clickstream-teaser.sqrl", "clickstream-teaser.graphqls", null,
//        "-c", PROJECT_ROOT.resolve("sqrl-testing/sqrl-integration-tests/src/test/resources/usecases/clickstream/package.json").toString());
//  }
//
//  @Test
//  public void testConference() {
//    execute("test", "conference", "conference.sqrl", "conference.graphqls", null,
//        "-c", PROJECT_ROOT.resolve("sqrl-testing/sqrl-integration-tests/src/test/resources/usecases/banking/package.json").toString());
//  }
//
//  @Test
//  public void testSensorsMutation() {
//    execute("test", "sensors", "sensors-mutation.sqrl", "sensors-mutation.graphqls", "sensors-mutation",
//        "-c", PROJECT_ROOT.resolve("sqrl-testing/sqrl-integration-tests/src/test/resources/usecases/sensors/package.json").toString());
//  }
//
//  @Test
//  @Disabled//still a bit flaky, need to move to somewhere with longer run times
//  public void testSensorsFull() {
//    execute("test", "sensors", "sensors-full.sqrl", null,"sensors-full",
//        "-c", PROJECT_ROOT.resolve("sqrl-testing/sqrl-integration-tests/src/test/resources/usecases/sensors/package.json").toString());
//  }
//
//  @Test
//  @Disabled//issue with CustomerPromotionTest (to debug)
//  public void testSeedshopExtended() {
//    execute("test", "seedshop-tutorial", "seedshop-extended.sqrl", null, "seedshop-extended",
//        "-c", PROJECT_ROOT.resolve("sqrl-testing/sqrl-integration-tests/src/test/resources/usecases/seedshop-tutorial/package.json").toString());
//  }
//
//  @SneakyThrows
//  @Test
//  @Disabled//OOMs in build server
//  public void testDuckdb() {
//
//    execute("test","duckdb", "duckdb.sqrl", null, "queries",
//        "-c", PROJECT_ROOT.resolve("sqrl-testing/sqrl-integration-tests/src/test/resources/usecases/duckdb/package.json").toString());
//
//  }
//
//  static DatasqrlRun datasqrlRun;
//
////
////  @BeforeAll
////  static void beforeAll() {
////    datasqrlRun = new DatasqrlRun();
//////    datasqrlRun.startKafkaCluster();
////  }
//
//  // Helper method to recursively delete a directory and all files
//
//  public void execute(String path, String script, String graphql) {
//    execute("test", path, script, graphql, null);
//  }
//
//  public void execute(String goal, String path, String script, String graphql, String testSuffix, String... args) {
//    Path rootDir = RESOURCES.resolve(path);
//    List<String> argsList = new ArrayList<>();
//    argsList.add(goal);
//    argsList.add(script);
//    if (!Strings.isNullOrEmpty(graphql)) argsList.add(graphql);
//    if (testSuffix!=null) {
//      argsList.add("-s"); argsList.add("snapshots-"+testSuffix);
//      argsList.add("--tests"); argsList.add("tests-"+testSuffix);
//    }
////    argsList.add("--profile");
////    argsList.add(getProjectRoot(rootDir).resolve("profiles/default").toString());
//    argsList.addAll(Arrays.asList(args));
//
//    execute(rootDir, new AssertStatusHook(), argsList.toArray(String[]::new));
//  }
//
//  protected void compile(String path, String script, String graphql) {
//    Path rootDir = RESOURCES.resolve(path);
//    List<String> argsList = new ArrayList<>();
//    argsList.add("compile");
//    argsList.add(script);
//    if (!Strings.isNullOrEmpty(graphql)) argsList.add(graphql);
//    argsList.add("--profile");
//    argsList.add(getProjectRoot(rootDir).resolve("profiles/default").toString());
//    execute(RESOURCES.resolve(path),
//        new AssertStatusHook(), argsList.toArray(a->new String[a]));
//  }
//
//  public int execute(Path rootDir, StatusHook hook, String... args) {
//    RootCommand rootCommand = new RootCommand(rootDir, hook);
//    int exitCode = rootCommand.getCmd().execute(args) + (hook.isSuccess() ? 0 : 1);
//    if (exitCode != 0) {
//      fail();
//    }
//
//    PackageJson packageJson = SqrlConfigCommons.fromFilesPackageJson(ErrorCollector.root(),
//        List.of(rootDir.resolve("build").resolve("package.json")));
//
//    TestEngines engines = new EngineFactory()
//        .create(packageJson);
//
//    this.containerHook = engines.accept(new TestContainersForTestGoal(), null);
//    containerHook.start();
//
//    Map<String, String> env = new HashMap<>();
//    env.put("EXECUTION_MODE", "local");
//    env.putAll(containerHook.getEnv());
//    env.put("DATA_PATH", rootDir.resolve("build/deploy/flink/data").toAbsolutePath().toString());
//
//    //Run the test
//    TestEnvContext context = TestEnvContext.builder()
//        .env(env)
//        .rootDir(rootDir)
//        .build();
////    engines.accept(new TestExecutionEnv(args[0], packageJson), context);
//
//    return exitCode;
//  }
//
//
//  protected static Path getProjectRoot(Path script) {
//    Path currentPath = script.toAbsolutePath();
//    while (!currentPath.getFileName().toString().equals("sqrl-testing")) {
//      currentPath = currentPath.getParent();
//    }
//
//    return currentPath.getParent();
//  }
//
//  protected static Path getProjectRoot() {
//    String userDir = System.getProperty("user.dir");
//    Path path = Path.of(userDir);
//    return getProjectRoot(path);
//  }
//}
