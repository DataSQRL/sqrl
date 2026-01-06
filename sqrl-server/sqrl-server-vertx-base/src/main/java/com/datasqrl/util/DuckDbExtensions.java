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

import static com.datasqrl.env.EnvVariableNames.DUCKDB_EXTENSIONS_DIR;

import com.datasqrl.graphql.config.JdbcConfig;
import java.util.Optional;
import java.util.StringJoiner;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public final class DuckDbExtensions {

  private final StringJoiner joiner = new StringJoiner(";", "", ";");

  private final JdbcConfig.DuckDbConfig config;

  public Optional<String> buildInitSql() {
    var extensionDir = System.getenv(DUCKDB_EXTENSIONS_DIR);

    if (extensionDir == null || extensionDir.trim().isEmpty()) {
      log.warn("Environment variable {} is not set, extensions will not be loaded.", extensionDir);
      return Optional.empty();
    }

    joiner.add("SET extension_directory='" + extensionDir + "'");
    joiner.add("LOAD iceberg");
    joiner.add("LOAD httpfs");

    if (config.isUseDiskCache()) {
      joiner.add("LOAD cache_httpfs");
    }

    if (config.isUseVersionGuessing()) {
      joiner.add("SET unsafe_enable_version_guessing = true");
    }

    return Optional.of(joiner.toString());
  }
}
