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
package com.datasqrl.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datasqrl.server.config.JdbcConfig;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DuckDbInitializerTest {

  private static final String EXTENSION_DIR = "/opt/duckdb_extensions";

  @Test
  void givenDefaultConfig_whenNoExtensionDir_thenSetsSpillDefaultsOnly() {
    var sql = new DuckDbInitializer(new JdbcConfig.DuckDbConfig()).buildInitSql(null);

    assertThat(sql)
        .isEqualTo(
            "SET temp_directory='"
                + DuckDbInitializer.DEFAULT_TEMP_DIRECTORY
                + "';"
                + "SET max_temp_directory_size='4GB';");
  }

  @Test
  void givenExtensionDir_whenBuildInitSql_thenSpillSettingsPrecedeExtensions() {
    var config = new JdbcConfig.DuckDbConfig();
    config.setMemoryLimit("2GB");
    config.setTempDirectory("/data/spill");
    config.setMaxTempDirectorySize("10GB");

    var sql = new DuckDbInitializer(config).buildInitSql(EXTENSION_DIR);

    assertThat(sql)
        .isEqualTo(
            "SET memory_limit='2GB';SET temp_directory='/data/spill';"
                + "SET max_temp_directory_size='10GB';"
                + "SET extension_directory='"
                + EXTENSION_DIR
                + "';LOAD iceberg;LOAD httpfs;");
  }

  @Test
  void givenCredentialChain_whenBuildInitSql_thenLoadsAwsAndCreatesRefreshingSecret() {
    var config = new JdbcConfig.DuckDbConfig();
    config.setUseCredentialChain(true);

    var sql = new DuckDbInitializer(config).buildInitSql(EXTENSION_DIR);

    assertThat(sql)
        .isEqualTo(
            "SET temp_directory='"
                + DuckDbInitializer.DEFAULT_TEMP_DIRECTORY
                + "';"
                + "SET max_temp_directory_size='4GB';"
                + "SET extension_directory='"
                + EXTENSION_DIR
                + "';LOAD iceberg;LOAD httpfs;"
                + "LOAD aws;"
                + "CREATE OR REPLACE SECRET sqrl_s3_credential_chain "
                + "(TYPE S3, PROVIDER credential_chain, REFRESH auto);");
  }

  @Test
  void givenCredentialChainWithoutExtensionDir_whenBuildInitSql_thenNoAwsStatements() {
    var config = new JdbcConfig.DuckDbConfig();
    config.setUseCredentialChain(true);

    assertThat(new DuckDbInitializer(config).buildInitSql(null))
        .doesNotContain("LOAD aws")
        .doesNotContain("SECRET");
  }

  @Test
  void givenHttpEndpoint_whenBuildInitSql_thenSecretUsesPathStyleWithoutSsl() {
    var sql = buildWithEndpoint(true, "http://s3mock:9090");

    assertThat(sql)
        .contains(
            "CREATE OR REPLACE SECRET sqrl_s3_credential_chain (TYPE S3, PROVIDER credential_chain, "
                + "REFRESH auto, ENDPOINT 's3mock:9090', URL_STYLE 'path', USE_SSL false);");
  }

  @Test
  void givenHttpsEndpoint_whenBuildInitSql_thenSecretUsesSsl() {
    var sql = buildWithEndpoint(true, "https://s3.example.com");

    assertThat(sql).contains("ENDPOINT 's3.example.com', URL_STYLE 'path', USE_SSL true)");
  }

  @Test
  void givenEndpointWithoutScheme_whenBuildInitSql_thenRejected() {
    assertThatThrownBy(() -> buildWithEndpoint(true, "s3mock:9090"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("http://");
  }

  @Test
  void givenEndpointWithoutCredentialChain_whenBuildInitSql_thenRejected() {
    assertThatThrownBy(() -> buildWithEndpoint(false, "http://s3mock:9090"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("use-credential-chain");
  }

  private static String buildWithEndpoint(boolean useCredentialChain, String endpoint) {
    var config = new JdbcConfig.DuckDbConfig();
    config.setUseCredentialChain(useCredentialChain);
    config.setS3Endpoint(endpoint);
    return new DuckDbInitializer(config).buildInitSql(EXTENSION_DIR);
  }

  @Test
  void givenTempDirectoryWithQuote_whenAppliedToDuckDb_thenDuckDbSeesExactPath(@TempDir Path tmp)
      throws Exception {
    var dir = tmp.resolve("it's").toString();
    var config = new JdbcConfig.DuckDbConfig();
    config.setTempDirectory(dir);

    try (var conn = DriverManager.getConnection("jdbc:duckdb:");
        var stmt = conn.createStatement()) {
      stmt.execute(new DuckDbInitializer(config).buildInitSql(null));

      try (var rs = stmt.executeQuery("SELECT current_setting('temp_directory')")) {
        assertThat(rs.next()).isTrue();
        assertThat(rs.getString(1)).isEqualTo(dir);
      }
    }
  }

  @Test
  void givenSmallSpillCap_whenQueryNeedsMoreDisk_thenDuckDbRejectsIt(@TempDir Path tmp)
      throws Exception {
    var config = new JdbcConfig.DuckDbConfig();
    config.setMemoryLimit("50MB");
    config.setTempDirectory(tmp.toString());
    config.setMaxTempDirectorySize("1MB");

    try (var conn = DriverManager.getConnection("jdbc:duckdb:");
        var stmt = conn.createStatement()) {
      stmt.execute(new DuckDbInitializer(config).buildInitSql(null));

      assertThatThrownBy(
              () ->
                  stmt.executeQuery(
                      "SELECT count(*) FROM (SELECT i, count(*) FROM range(5000000) r(i) GROUP BY i)"))
          .isInstanceOf(SQLException.class)
          .hasMessageContaining("failed to offload");
    }
  }
}
