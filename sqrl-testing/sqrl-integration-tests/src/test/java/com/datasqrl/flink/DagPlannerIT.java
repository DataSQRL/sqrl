package com.datasqrl.flink;

import static org.junit.jupiter.api.Assertions.fail;

import com.datasqrl.DatasqrlRun;
import com.datasqrl.cmd.AssertStatusHook;
import com.datasqrl.cmd.RootCommand;
import java.io.File;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.table.api.CompiledPlan;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.redpanda.RedpandaContainer;
import org.testcontainers.utility.DockerImageName;

@Slf4j
@TestInstance(TestInstance.Lifecycle.PER_CLASS) // This is to allow the method source to not be static
@Testcontainers
public class DagPlannerIT {
  @Container
  private PostgreSQLContainer testDatabase =
      new PostgreSQLContainer(DockerImageName.parse("ankane/pgvector:v0.5.0")
          .asCompatibleSubstituteFor("postgres"))
          .withDatabaseName("foo")
          .withUsername("foo")
          .withPassword("secret")
          .withDatabaseName("datasqrl");

  @Container
  RedpandaContainer testKafka =
      new RedpandaContainer("docker.redpanda.com/redpandadata/redpanda:v23.1.2");

  // Method to provide the directories as test arguments
  static Stream<Path> directoryProvider() {
    Path firstDirPath = Path.of("../sqrl-integration-tests/src/test/resources/dagplanner");
    Stream<Path> firstDirStream = getDirectoriesStream(firstDirPath);

//    Path secondDirPath = Path.of("../sqrl-integration-tests/src/test/resources/usecases/plan");
//    Stream<Path> secondDirStream = getDirectoriesStream(secondDirPath);

    return firstDirStream;
  }

  private static Stream<Path> getDirectoriesStream(Path rootPath) {
    File directory = rootPath.toFile();
    if (directory.exists() && directory.isDirectory()) {
      File[] sqrlFiles = directory.listFiles(f->
          f.getName().endsWith(".sqrl") && !f.getName().contains("disabled") && !f.getName().contains("fail"));
      if (sqrlFiles != null) {
        return Stream.of(sqrlFiles).map(File::toPath);
      }
    }
    return Stream.empty();
  }

  List<String> disabled = List.of(
      "nestedAggregationAndSelfJoinTest.sqrl", //Processing-time temporal join is not supported yet
      "selectDistinctNestedTest.sqrl", //Nested query produces invalid sql plan
      "timestampReassignment.sqrl"//Event-Time Temporal Table Join requires both primary key and row time attribute in versioned table, but no row time attribute can be found.
  );

  @ParameterizedTest
  @MethodSource("directoryProvider")
  void testCompilePlanOnDirectory(Path directoryPath) {
    Map<String, String> env = new HashMap<>();
    env.put("EXECUTION_MODE", "local");
    env.put("JDBC_URL", testDatabase.getJdbcUrl());
    env.put("JDBC_AUTHORITY", testDatabase.getJdbcUrl().split("://")[1]);
    env.put("PGHOST", testDatabase.getHost());
    env.put("PGUSER", testDatabase.getUsername());
    env.put("JDBC_USERNAME", testDatabase.getUsername());
    env.put("JDBC_PASSWORD", testDatabase.getPassword());
    env.put("PGPORT", testDatabase.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT).toString());
    env.put("PGPASSWORD", testDatabase.getPassword());
    env.put("PGDATABASE", testDatabase.getDatabaseName());
    env.put("PROPERTIES_BOOTSTRAP_SERVERS", testKafka.getBootstrapServers());

    DatasqrlRun datasqrlRun = new DatasqrlRun(directoryPath.getParent().resolve("build").resolve("plan"), env);

    if (disabled.contains(directoryPath.getFileName().toString())){
      log.warn("Skipping Disabled Test");
      return;
    }

    AssertStatusHook statusHook = new AssertStatusHook();
    int code = new RootCommand(directoryPath.getParent(),statusHook).getCmd().execute("compile",
        directoryPath.getFileName().toString());
    if (statusHook.isSuccess()) Assertions.assertEquals(0, code);

    try {
      CompiledPlan plan = datasqrlRun.compileFlink();
      plan.explain(); //invoke to make flink do extra validation
      // plan.execute().print(); Uncomment if execution is required
    } catch (Exception e) {
      fail("Failed to compile plan for directory: " + directoryPath, e);
    }
  }
}