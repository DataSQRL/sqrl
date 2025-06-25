/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasqrl;

import static org.assertj.core.api.Assertions.fail;

import com.datasqrl.cli.AssertStatusHook;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

/**
 * Compiles the use cases in the test/resources/usecases folder and snapshots the deployment assets
 */
@Slf4j
public class PostgresViewIT extends AbstractUseCaseTest {
  @Container
  private PostgreSQLContainer testDatabase =
      new PostgreSQLContainer(
              DockerImageName.parse("ankane/pgvector:v0.5.0").asCompatibleSubstituteFor("postgres"))
          .withDatabaseName("foo")
          .withUsername("foo")
          .withPassword("secret")
          .withDatabaseName("datasqrl");

  public static final Path USECASE_DIR = getResourcesDirectory("usecases");

  @Override
  @SneakyThrows
  @ParameterizedTest
  @ArgumentsSource(UseCaseFiles.class)
  void testUsecase(Path script, Path graphQlFile, Path packageFile) {
    super.testUsecase(script, graphQlFile, packageFile);
    try {
      verifyPostgresSchema(script);
    } catch (Exception e) {
      e.printStackTrace();
      fail("", e);
    }
  }

  @Override
  public void snapshot(String testname, AssertStatusHook hook) {
    // nothing to snapshot
  }

  private void verifyPostgresSchema(Path script) throws Exception {
    var file = script.getParent().resolve("build/plan/postgres.json").toFile();
    if (file.exists()) {
      testDatabase.start();
      var plan = new ObjectMapper().readValue(file, Map.class);
      for (Map statement : (List<Map>) plan.get("statements")) {
        log.info("Executing statement {} of type {}", statement.get("name"), statement.get("type"));
        try (var connection = testDatabase.createConnection("")) {
          try (var stmt = connection.createStatement()) {
            stmt.executeUpdate((String) statement.get("sql"));
          }
        }
      }
    }
  }

  private static List<String> getTables(PostgreSQLContainer testDatabase) throws Exception {
    List<String> result = new ArrayList<>();
    try (var connection = testDatabase.createConnection("")) {
      var metaData = connection.getMetaData();
      var tables = metaData.getTables(null, null, "%", new String[] {"TABLE"});

      while (tables.next()) {
        result.add(tables.getString("TABLE_NAME"));
      }
    }
    return result;
  }

  static class UseCaseFiles extends SqrlScriptsAndLocalPackages {
    public UseCaseFiles() {
      super(USECASE_DIR, true);
    }
  }
}
