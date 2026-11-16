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
package com.datasqrl.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datasqrl.error.CollectedException;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.util.ConfigLoaderUtils;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class CompilerApiConfigImplTest {

  private static final String PREFIX_KEY = "mcp-resource-uri-prefix";

  private SqrlConfig config;

  @BeforeEach
  void setUp() {
    config = SqrlConfig.createCurrentVersion();
  }

  @ParameterizedTest
  @ValueSource(strings = {"sqrl://{version}/", "https://api.example.com/", "urn:acme:"})
  void givenPrefixWithScheme_whenGetMcpResourceUriPrefix_thenReturnsPrefix(String prefix) {
    config.setProperty(PREFIX_KEY, prefix);

    assertThat(new CompilerApiConfigImpl(config).getMcpResourceUriPrefix()).isEqualTo(prefix);
  }

  @ParameterizedTest
  @ValueSource(strings = {"queries/", "/v1/", "://missing", "1abc://x/"})
  void givenPrefixWithoutScheme_whenGetMcpResourceUriPrefix_thenThrowsReadableError(String prefix) {
    config.setProperty(PREFIX_KEY, prefix);

    assertThatThrownBy(() -> new CompilerApiConfigImpl(config).getMcpResourceUriPrefix())
        .isInstanceOf(CollectedException.class)
        .hasMessageContaining(PREFIX_KEY)
        .hasMessageContaining("must start with a URI scheme");
  }

  @ParameterizedTest
  @ValueSource(strings = {"ui://app/", "UI://app/"})
  void givenMcpAppsScheme_whenGetMcpResourceUriPrefix_thenThrowsReadableError(String prefix) {
    config.setProperty(PREFIX_KEY, prefix);

    assertThatThrownBy(() -> new CompilerApiConfigImpl(config).getMcpResourceUriPrefix())
        .isInstanceOf(CollectedException.class)
        .hasMessageContaining(PREFIX_KEY)
        .hasMessageContaining("reserved for MCP Apps");
  }

  @Test
  void givenDefaultPackage_whenGetMcpResourceUriPrefix_thenReturnsSqrlScheme() {
    var packageJson =
        ConfigLoaderUtils.loadUnresolvedConfig(
            ErrorCollector.root(),
            List.of(Path.of("src/test/resources/config/package/compilerTags.json")));

    assertThat(packageJson.getCompilerConfig().getApiConfig().getMcpResourceUriPrefix())
        .isEqualTo("sqrl://{version}/");
  }

  @Test
  void givenNoPrefix_whenGetMcpResourceUriPrefix_thenThrowsMissingKey() {
    assertThatThrownBy(() -> new CompilerApiConfigImpl(config).getMcpResourceUriPrefix())
        .isInstanceOf(CollectedException.class)
        .hasMessageContaining(PREFIX_KEY);
  }
}
