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

import com.datasqrl.graphql.config.JdbcConfig;
import org.junit.jupiter.api.Test;

class DuckDbExtensionsTest {

  private static final String EXTENSION_DIR = "/opt/duckdb_extensions";

  @Test
  void givenDefaultConfig_whenBuildInitSql_thenLoadsCoreExtensionsOnly() {
    var sql = new DuckDbExtensions(new JdbcConfig.DuckDbConfig()).buildInitSql(EXTENSION_DIR);

    assertThat(sql)
        .isEqualTo("SET extension_directory='" + EXTENSION_DIR + "';LOAD iceberg;LOAD httpfs;");
    assertThat(sql).doesNotContain("LOAD aws").doesNotContain("CREATE OR REPLACE SECRET");
  }

  @Test
  void givenCredentialChainEnabled_whenBuildInitSql_thenLoadsAwsAndCreatesS3Secret() {
    var config = new JdbcConfig.DuckDbConfig();
    config.setUseCredentialChain(true);

    var sql = new DuckDbExtensions(config).buildInitSql(EXTENSION_DIR);

    assertThat(sql)
        .contains("LOAD aws;")
        .contains(
            "CREATE OR REPLACE SECRET sqrl_s3_credential_chain (TYPE S3, PROVIDER credential_chain);");
    assertThat(sql)
        .as(
            "must use the default chain; an explicit CHAIN with 'sts' is rejected without ASSUME_ROLE_ARN")
        .doesNotContain("CHAIN '");
  }

  @Test
  void givenCredentialChainEnabled_whenBuildInitSql_thenLoadsAwsAfterHttpfsAndBeforeSecret() {
    var config = new JdbcConfig.DuckDbConfig();
    config.setUseCredentialChain(true);

    var sql = new DuckDbExtensions(config).buildInitSql(EXTENSION_DIR);

    assertThat(sql.indexOf("LOAD httpfs"))
        .isLessThan(sql.indexOf("LOAD aws"))
        .isGreaterThanOrEqualTo(0);
    assertThat(sql.indexOf("LOAD aws")).isLessThan(sql.indexOf("CREATE OR REPLACE SECRET"));
  }

  @Test
  void givenAllFlagsEnabled_whenBuildInitSql_thenAppliesEveryStatement() {
    var config = new JdbcConfig.DuckDbConfig();
    config.setUseCredentialChain(true);
    config.setUseDiskCache(true);
    config.setUseVersionGuessing(true);

    var sql = new DuckDbExtensions(config).buildInitSql(EXTENSION_DIR);

    assertThat(sql)
        .contains("LOAD aws;")
        .contains("LOAD cache_httpfs;")
        .contains("SET unsafe_enable_version_guessing = true;");
  }
}
