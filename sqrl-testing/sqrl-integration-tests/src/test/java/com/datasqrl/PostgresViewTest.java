package com.datasqrl;

import static org.junit.jupiter.api.Assertions.fail;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import com.datasqrl.cmd.AssertStatusHook;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.SneakyThrows;

/**
 * Compiles the use cases in the test/resources/usecases folder and snapshots the
 * deployment assets
 */
public class PostgresViewTest extends AbstractUseCaseTest {
  @Container
  private PostgreSQLContainer testDatabase =
      new PostgreSQLContainer(DockerImageName.parse("ankane/pgvector:v0.5.0")
          .asCompatibleSubstituteFor("postgres"))
          .withDatabaseName("foo")
          .withUsername("foo")
          .withPassword("secret")
          .withDatabaseName("datasqrl");

  public static final Path USECASE_DIR = getResourcesDirectory("usecases");

  protected PostgresViewTest() {
    super(USECASE_DIR);
  }

  @Override
@SneakyThrows
  @ParameterizedTest
  @ArgumentsSource(UseCaseFiles.class)
  void testUsecase(Path script, Path graphQlFile, Path packageFile) {
    super.testUsecase(script, graphQlFile, packageFile);
    try {
      verifyPostgresSchema(script);
    } catch (Exception e) {
      fail(e);
    }
  }

  @Override
  public void snapshot(String testname, AssertStatusHook hook) {
    //nothing to snapshot
  }

  private void verifyPostgresSchema(Path script) throws Exception {
    var file = script.getParent().resolve("build/plan/postgres.json").toFile();
    if (file.exists()) {
      testDatabase.start();
      var plan = new ObjectMapper().readValue(file, Map.class);
      for (Map statement : (List<Map>) plan.get("ddl")) {
        var connection = testDatabase.createConnection("");
        try (var stmt = connection.createStatement()) {
          stmt.executeUpdate((String) statement.get("sql"));
        }
      }
      if (plan.get("views") != null) {
        for (Map statement : (List<Map>) plan.get("views")) {
          var connection = testDatabase.createConnection("");
          try (var stmt = connection.createStatement()) {
            stmt.executeUpdate((String) statement.get("sql"));
          }
        }
      }
    }
  }

  static class UseCaseFiles extends SqrlScriptsAndLocalPackages {
    public UseCaseFiles() {
      super(USECASE_DIR, true);
    }
  }
}
