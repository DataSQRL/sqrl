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

import com.datasqrl.server.config.JdbcConfig;
import jakarta.annotation.Nullable;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.StringJoiner;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

@RequiredArgsConstructor
@Slf4j
public final class DuckDbInitializer {

  /**
   * DuckDB's own default is {@code .tmp} under the working directory, which is the read-only config
   * mount in the server image.
   */
  public static final String DEFAULT_TEMP_DIRECTORY =
      Path.of(System.getProperty("java.io.tmpdir"), "sqrl-duckdb").toString();

  private static final String INVALID_S3_ENDPOINT =
      "s3-endpoint must be an http:// or https:// URL, for example http://localhost:9090";

  private static final String CREDENTIAL_CHAIN_SECRET_OPTIONS =
      "TYPE S3, PROVIDER credential_chain, REFRESH auto";

  private final JdbcConfig.DuckDbConfig config;

  public String buildInitSql() {
    var extensionDir = System.getenv(DUCKDB_EXTENSIONS_DIR);

    if (StringUtils.isBlank(extensionDir)) {
      log.warn(
          "Environment variable {} is not set, extensions will not be loaded.",
          DUCKDB_EXTENSIONS_DIR);
      extensionDir = null;
    }

    return buildInitSql(extensionDir);
  }

  /**
   * Builds the init SQL; a {@code null} extension directory skips loading extensions.
   *
   * <p>Every setting is a {@code SET GLOBAL}: the init SQL runs on one connection and queries run
   * on duplicates of it, which do not see session settings such as a plain {@code SET} of an
   * extension option.
   */
  String buildInitSql(@Nullable String extensionDir) {
    var s3EndpointOptions = s3EndpointOptions();
    var joiner = new StringJoiner(";", "", ";");

    if (StringUtils.isNotBlank(config.getMemoryLimit())) {
      joiner.add("SET GLOBAL memory_limit=" + sqlString(config.getMemoryLimit()));
    }

    var tempDirectory =
        StringUtils.isNotBlank(config.getTempDirectory())
            ? config.getTempDirectory()
            : DEFAULT_TEMP_DIRECTORY;
    joiner.add("SET GLOBAL temp_directory=" + sqlString(tempDirectory));
    joiner.add("SET GLOBAL max_temp_directory_size=" + sqlString(config.getMaxTempDirectorySize()));

    if (extensionDir != null) {
      addExtensions(joiner, extensionDir, s3EndpointOptions);
    }

    return joiner.toString();
  }

  /** Secret options for {@code s3-endpoint}, or an empty string when it is not set. */
  private String s3EndpointOptions() {
    var endpoint = config.getS3Endpoint();
    if (StringUtils.isBlank(endpoint)) {
      return "";
    }
    if (!config.isUseCredentialChain()) {
      throw new IllegalArgumentException("s3-endpoint requires use-credential-chain");
    }

    URI uri;
    try {
      uri = new URI(endpoint);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(INVALID_S3_ENDPOINT, e);
    }
    var scheme = uri.getScheme();
    if (!("http".equals(scheme) || "https".equals(scheme)) || uri.getAuthority() == null) {
      throw new IllegalArgumentException(INVALID_S3_ENDPOINT);
    }

    return ", ENDPOINT "
        + sqlString(uri.getAuthority())
        + ", URL_STYLE 'path', USE_SSL "
        + "https".equals(scheme);
  }

  private void addExtensions(StringJoiner joiner, String extensionDir, String s3EndpointOptions) {
    joiner.add("SET GLOBAL extension_directory=" + sqlString(extensionDir));
    joiner.add("LOAD iceberg");
    joiner.add("LOAD httpfs");

    if (config.isUseCredentialChain()) {
      // The default chain covers environment variables, profiles and the EKS IRSA web-identity
      // token. With REFRESH auto, DuckDB re-runs the chain when S3 rejects expired credentials.
      joiner.add("LOAD aws");
      joiner.add(
          "CREATE OR REPLACE SECRET sqrl_s3_credential_chain ("
              + CREDENTIAL_CHAIN_SECRET_OPTIONS
              + s3EndpointOptions
              + ")");
    }

    if (config.isUseDiskCache()) {
      joiner.add("LOAD cache_httpfs");
    }

    if (config.isUseVersionGuessing()) {
      joiner.add("SET GLOBAL unsafe_enable_version_guessing = true");
    }
  }

  private static String sqlString(String value) {
    return "'" + value.replace("'", "''") + "'";
  }
}
