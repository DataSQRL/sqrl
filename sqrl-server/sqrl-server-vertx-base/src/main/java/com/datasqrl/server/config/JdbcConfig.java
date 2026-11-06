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
package com.datasqrl.server.config;

import static com.google.common.base.Preconditions.checkNotNull;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class JdbcConfig {

  private String url;

  public void validateConfig() {
    checkNotNull(url, "The 'url' config must be set");
  }

  @Getter
  @Setter
  @NoArgsConstructor
  public static class DuckDbConfig extends JdbcConfig {

    @JsonProperty("use-disk-cache")
    private boolean useDiskCache;

    @JsonProperty("use-version-guessing")
    private boolean useVersionGuessing;

    /** Creates an S3 secret from the AWS credential provider chain that DuckDB refreshes. */
    @JsonProperty("use-credential-chain")
    private boolean useCredentialChain;

    /** URL of an S3-compatible store such as S3Mock, for example {@code http://localhost:9090}. */
    @JsonProperty("s3-endpoint")
    private String s3Endpoint;

    @JsonProperty("memory-limit")
    private String memoryLimit;

    /** Number of duplicated connections, so the number of queries DuckDB runs at once. */
    @JsonProperty("pool-size")
    private int poolSize = 4;

    /**
     * Directory DuckDB spills to; {@code null} means a {@code sqrl-duckdb} dir in java.io.tmpdir.
     */
    @JsonProperty("temp-directory")
    private String tempDirectory;

    @JsonProperty("max-temp-directory-size")
    private String maxTempDirectorySize = "4GB";
  }
}
