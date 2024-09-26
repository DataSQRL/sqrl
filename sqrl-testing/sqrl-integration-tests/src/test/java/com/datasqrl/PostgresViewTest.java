package com.datasqrl;

import static org.junit.jupiter.api.Assertions.fail;

import com.datasqrl.cmd.AssertStatusHook;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import lombok.SneakyThrows;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

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
    File file = script.getParent().resolve("build/plan/postgres.json").toFile();
    if (file.exists()) {
      testDatabase.start();
      Map plan = new ObjectMapper().readValue(file, Map.class);
      for (Map statement : (List<Map>) plan.get("ddl")) {
        Connection connection = testDatabase.createConnection("");
        try (Statement stmt = connection.createStatement()) {
          stmt.executeUpdate((String) statement.get("sql"));
        }
      }
      if (plan.get("views") != null) {
        for (Map statement : (List<Map>) plan.get("views")) {
          Connection connection = testDatabase.createConnection("");
          try (Statement stmt = connection.createStatement()) {
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
