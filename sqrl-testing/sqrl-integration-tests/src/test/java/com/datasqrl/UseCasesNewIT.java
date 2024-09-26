//package com.datasqrl;
//
//import static org.junit.jupiter.api.Assertions.fail;
//
//import com.datasqrl.cmd.AssertStatusHook;
//import com.datasqrl.cmd.RootCommand;
//import com.datasqrl.cmd.StatusHook;
//import java.io.IOException;
//import java.nio.file.Files;
//import java.nio.file.Path;
//import java.nio.file.Paths;
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import lombok.SneakyThrows;
//import org.apache.flink.calcite.shaded.com.google.common.base.Strings;
//import org.apache.flink.test.junit5.MiniClusterExtension;
//import org.junit.jupiter.api.AfterEach;
//import org.junit.jupiter.api.BeforeEach;
//import org.junit.jupiter.api.Disabled;
//import org.junit.jupiter.api.Test;
//import org.junit.jupiter.api.extension.ExtendWith;
//import org.testcontainers.containers.PostgreSQLContainer;
//import org.testcontainers.junit.jupiter.Testcontainers;
//import org.testcontainers.redpanda.RedpandaContainer;
//import org.testcontainers.utility.DockerImageName;
//
///**
// * Tests some use cases in the test/resources/usecases folder using the `test` command.
// */
//@Testcontainers
//@ExtendWith(MiniClusterExtension.class)
//public class UseCasesNewIT {
//  private PostgreSQLContainer testDatabase;
//
//  RedpandaContainer testKafka;
//
//  protected static final Path PROJECT_ROOT = getProjectRoot();
//  private static final Path RESOURCES = Paths.get("src/test/resources/usecases");
//  @BeforeEach
//  public void setUp() {
//    // Start a new PostgreSQL container for each test
//    testDatabase = new PostgreSQLContainer<>(DockerImageName.parse("ankane/pgvector:v0.5.0")
//        .asCompatibleSubstituteFor("postgres"))
//        .withDatabaseName("datasqrl")
//        .withUsername("foo")
//        .withPassword("secret");
//    testDatabase.start();
//
//    // Start a new Redpanda container for each test
//    testKafka = new RedpandaContainer("docker.redpanda.com/redpandadata/redpanda:v23.1.2");
//    testKafka.start();
//  }
//
//  @AfterEach
//  public void tearDown() throws Exception {
//    testDatabase.stop();
//    testKafka.stop();
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
////  @Disabled //todo needs more time?
//  public void testSensorsMutation() {
//    execute("test", "sensors", "sensors-mutation.sqrl", "sensors-mutation.graphqls", "sensors-mutation",
//        "-c", PROJECT_ROOT.resolve("sqrl-testing/sqrl-integration-tests/src/test/resources/usecases/sensors/package.json").toString());
//  }
//
//  @Test
//  @Disabled //todo needs more time
//  public void testSensorsFull() {
//    execute("test", "sensors", "sensors-full.sqrl", null,"sensors-full",
//        "-c", PROJECT_ROOT.resolve("sqrl-testing/sqrl-integration-tests/src/test/resources/usecases/sensors/package.json").toString());
//  }
//
//  @Test
//  @Disabled //todo fix CustomerPromotionTest
//  public void testSeedshopExtended() {
//    execute("test", "seedshop-tutorial", "seedshop-extended.sqrl", null, "seedshop-extended",
//        "-c", PROJECT_ROOT.resolve("sqrl-testing/sqrl-integration-tests/src/test/resources/usecases/seedshop-tutorial/package.json").toString());
//  }
//
//  @SneakyThrows
//  @Test
//  @Disabled
//  public void testDuckdb() {
//    Path path = Path.of("/tmp/duckdb");
//    try {
//      deleteDirectory(path);
//    } catch (Exception e) {}
//
//    Files.createDirectories(path);
//
//    execute("test","duckdb", "duckdb.sqrl", null, "queries",
//        "-c", PROJECT_ROOT.resolve("sqrl-testing/sqrl-integration-tests/src/test/resources/usecases/duckdb/package.json").toString());
//
//    deleteDirectory(path);
//  }
//
//  // Helper method to recursively delete a directory and all files
//  private void deleteDirectory(Path directory) throws IOException {
//    Files.walk(directory)
//        .sorted((path1, path2) -> path2.compareTo(path1)) // Sort in reverse order to delete files first
//        .forEach(path -> {
//          try {
//            Files.delete(path);
//          } catch (IOException e) {
//            throw new RuntimeException("Failed to delete " + path, e);
//          }
//        });
//  }
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
//    Map<String, String> env = new HashMap<>();
//    env.put("EXECUTION_MODE", "local");
//    env.put("JDBC_URL", testDatabase.getJdbcUrl());
//    env.put("PGHOST", testDatabase.getHost());
//    env.put("PGUSER", testDatabase.getUsername());
//    env.put("JDBC_USERNAME", testDatabase.getUsername());
//    env.put("JDBC_PASSWORD", testDatabase.getPassword());
//    env.put("PGPORT", testDatabase.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT).toString());
//    env.put("PGPASSWORD", testDatabase.getPassword());
//    env.put("PGDATABASE", testDatabase.getDatabaseName());
//    env.put("PROPERTIES_BOOTSTRAP_SERVERS", testKafka.getBootstrapServers());
//
//    env.put("DATA_PATH", rootDir.resolve("build/deploy/flink/data").toAbsolutePath().toString());
//    RootCommand rootCommand = new RootCommand(rootDir, hook);
//    int exitCode = rootCommand.getCmd().execute(args) + (hook.isSuccess() ? 0 : 1);
//    if (exitCode != 0) {
//      fail();
//    }
//
//    DatasqrlTest test = new DatasqrlTest(null, rootDir.resolve("build/plan"), env);
//    int run = test.run();
//    if (run != 0) {
//      fail();
//    }
//    return exitCode;
//  }
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
