package com.datasqrl;

import static org.junit.jupiter.api.Assertions.fail;

import com.datasqrl.cmd.AssertStatusHook;
import com.datasqrl.cmd.RootCommand;
import com.datasqrl.cmd.StatusHook;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.flink.calcite.shaded.com.google.common.base.Strings;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.redpanda.RedpandaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Tests some use cases in the test/resources/usecases folder using the `test` command.
 */
@Testcontainers
public class UseCasesIT {
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

  protected static final Path PROJECT_ROOT = getProjectRoot();
  private static final Path RESOURCES = Paths.get("src/test/resources/usecases");


  @Test //Done
  public void testRepository() {
    execute("repository", "repo.sqrl", "repo.graphqls");
  }

  @Test
  public void testLoggingToKafka() {
    execute("logging/it-kafka", "logging-kafka.sqrl", "logging-kafka.graphqls");
  }

  public void execute(String path, String script, String graphql) {
    execute("test", path, script, graphql, null);
  }

  public void execute(String goal, String path, String script, String graphql, String testSuffix, String... args) {
    Path rootDir = RESOURCES.resolve(path);
    List<String> argsList = new ArrayList<>();
    argsList.add(goal);
    argsList.add(script);
    if (!Strings.isNullOrEmpty(graphql)) argsList.add(graphql);
    if (testSuffix!=null) {
      argsList.add("-s"); argsList.add("snapshots-"+testSuffix);
      argsList.add("--tests"); argsList.add("tests-"+testSuffix);
    }
    argsList.add("--profile");
    argsList.add(getProjectRoot(rootDir).resolve("profiles/default").toString());
    argsList.addAll(Arrays.asList(args));

    execute(rootDir, AssertStatusHook.INSTANCE, argsList.toArray(String[]::new));
  }

  protected void compile(String path, String script, String graphql) {
    Path rootDir = RESOURCES.resolve(path);
    List<String> argsList = new ArrayList<>();
    argsList.add("compile");
    argsList.add(script);
    if (!Strings.isNullOrEmpty(graphql)) argsList.add(graphql);
    argsList.add("--profile");
    argsList.add(getProjectRoot(rootDir).resolve("profiles/default").toString());
    execute(RESOURCES.resolve(path),
        AssertStatusHook.INSTANCE, argsList.toArray(a->new String[a]));
  }

  public int execute(Path rootDir, StatusHook hook, String... args) {

    Map<String, String> env = Map.of(
        "JDBC_URL", testDatabase.getJdbcUrl(),
        "PGHOST", testDatabase.getHost(),
        "PGUSER", testDatabase.getUsername(),
        "JDBC_USERNAME", testDatabase.getUsername(),
        "JDBC_PASSWORD", testDatabase.getPassword(),
        "PGPORT", testDatabase.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT).toString(),
        "PGPASSWORD", testDatabase.getPassword(),
        "PGDATABASE", testDatabase.getDatabaseName(),
        "PROPERTIES_BOOTSTRAP_SERVERS", container.getBootstrapServers(),
        "JMETER_HOME", "/Users/henneberger/Downloads/apache-jmeter-5.6.3"
    );
    RootCommand rootCommand = new RootCommand(rootDir, hook, env);
    int exitCode = rootCommand.getCmd().execute(args) + (hook.isSuccess() ? 0 : 1);
    if (exitCode != 0) {
      fail();
    }
    return exitCode;
  }

  protected static Path getProjectRoot(Path script) {
    Path currentPath = script.toAbsolutePath();
    while (!currentPath.getFileName().toString().equals("sqrl-testing")) {
      currentPath = currentPath.getParent();
    }

    return currentPath.getParent();
  }

  protected static Path getProjectRoot() {
    String userDir = System.getProperty("user.dir");
    Path path = Path.of(userDir);
    return getProjectRoot(path);
  }
}
