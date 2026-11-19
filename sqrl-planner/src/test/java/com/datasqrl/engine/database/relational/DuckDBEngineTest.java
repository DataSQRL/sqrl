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
package com.datasqrl.engine.database.relational;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import com.datasqrl.config.ConnectorFactoryFactory;
import com.datasqrl.error.CollectedException;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.util.ConfigLoaderUtils;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** The check of {@code s3-endpoint} against {@code use-credential-chain} on merged packages. */
class DuckDBEngineTest {

  @TempDir private Path tempDir;

  private final ErrorCollector errors = ErrorCollector.root();

  @Test
  void givenCredentialChainInBaseAndS3EndpointInOverlay_whenEngineCreated_thenAccepted() {
    createEngine(
        "{\"url\": \"jdbc:duckdb:\", \"use-credential-chain\": true}",
        "{\"url\": \"jdbc:duckdb:\", \"s3-endpoint\": \"http://s3mock:9090\"}");

    assertThat(errors).isEmpty();
  }

  @Test
  void givenS3EndpointWithoutCredentialChain_whenEngineCreated_thenRejected() {
    assertThatThrownBy(
            () ->
                createEngine(
                    "{\"url\": \"jdbc:duckdb:\", \"s3-endpoint\": \"http://s3mock:9090\"}"))
        .isInstanceOf(CollectedException.class)
        .hasMessageContaining("'s3-endpoint'")
        .hasMessageContaining("'use-credential-chain': true");
  }

  @Test
  void givenBlankS3EndpointWithoutCredentialChain_whenEngineCreated_thenAccepted() {
    createEngine("{\"url\": \"jdbc:duckdb:\", \"s3-endpoint\": \"\"}");

    assertThat(errors).isEmpty();
  }

  /** Loads the package files as {@code sqrl compile} does, each with the given duckdb engine. */
  @SneakyThrows
  private DuckDBEngine createEngine(String... duckDbConfigs) {
    var packageFiles = new ArrayList<Path>();
    for (var duckDbConfig : duckDbConfigs) {
      var packageFile = tempDir.resolve("package-" + packageFiles.size() + ".json");
      Files.writeString(
          packageFile, "{\"version\": \"1\", \"engines\": {\"duckdb\": " + duckDbConfig + "}}");
      packageFiles.add(packageFile);
    }

    var packageJson = ConfigLoaderUtils.loadUnresolvedConfig(errors, packageFiles);

    return new DuckDBEngine(packageJson, mock(ConnectorFactoryFactory.class), errors);
  }
}
