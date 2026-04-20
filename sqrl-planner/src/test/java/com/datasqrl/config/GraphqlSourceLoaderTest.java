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

import com.datasqrl.config.PackageJson.ScriptApiConfig;
import com.datasqrl.config.PackageJson.ScriptConfig;
import com.datasqrl.graphql.ApiSource;
import com.datasqrl.graphql.ScriptFiles;
import com.datasqrl.loaders.resolver.FileResourceResolver;
import com.datasqrl.loaders.resolver.ResourceResolver;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class GraphqlSourceLoaderTest {

  @TempDir Path tempDir;

  private ResourceResolver resolver;

  @BeforeEach
  void setUp() {
    resolver = new FileResourceResolver(tempDir);
  }

  @Test
  void givenGraphqlAndOperations_whenConstruct_thenDefaultOperationsEmpty() throws IOException {
    Files.writeString(tempDir.resolve("schema.graphqls"), "type Query { foo: String }");
    Files.writeString(tempDir.resolve("op.graphql"), "query Foo { foo }");
    var scriptFiles = scriptFiles("schema.graphqls", List.of("op.graphql"), List.of());

    var loader = new GraphqlSourceLoader(scriptFiles, resolver);

    assertThat(loader.getApiVersions()).hasSize(1);
    assertThat(loader.getApiVersions().get(0).operations())
        .extracting(ApiSource::getDefinition)
        .containsExactly("query Foo { foo }");
    assertThat(loader.getDefaultOperations()).isEmpty();
  }

  @Test
  void givenApiConfigs_whenConstruct_thenDefaultOperationsEmpty() throws IOException {
    Files.writeString(tempDir.resolve("v1-schema.graphqls"), "type Query { foo: String }");
    var apiConfig = apiConfig(List.of());
    var scriptFiles = scriptFiles(null, List.of("ignored.graphql"), List.of(apiConfig));

    var loader = new GraphqlSourceLoader(scriptFiles, resolver);

    assertThat(loader.getApiVersions()).hasSize(1);
    assertThat(loader.getApiByVersion()).containsOnlyKeys("v1");
    assertThat(loader.getDefaultOperations()).isEmpty();
  }

  @Test
  void givenOperationsWithoutGraphql_whenCreateInferred_thenUsesDefaultOperations()
      throws IOException {
    Files.writeString(tempDir.resolve("op.graphql"), "query Foo { foo }");
    var scriptFiles = scriptFiles(null, List.of("op.graphql"), List.of());
    var loader = new GraphqlSourceLoader(scriptFiles, resolver);

    var result = loader.createInferredApiSources("type Query { foo: String }");

    assertThat(result.schema().getDefinition()).isEqualTo("type Query { foo: String }");
    assertThat(result.operations())
        .extracting(ApiSource::getDefinition)
        .containsExactly("query Foo { foo }");
  }

  @Test
  void givenGraphqlAndOperations_whenCreateInferred_thenPreservesExistingOperations()
      throws IOException {
    Files.writeString(tempDir.resolve("schema.graphqls"), "type Query { foo: String }");
    Files.writeString(tempDir.resolve("op.graphql"), "query Foo { foo }");
    var scriptFiles = scriptFiles("schema.graphqls", List.of("op.graphql"), List.of());
    var loader = new GraphqlSourceLoader(scriptFiles, resolver);

    var result = loader.createInferredApiSources("type Query { bar: String }");

    assertThat(result.schema().getDefinition()).isEqualTo("type Query { bar: String }");
    assertThat(result.operations())
        .extracting(ApiSource::getDefinition)
        .containsExactly("query Foo { foo }");
  }

  @Test
  void givenNoOperations_whenCreateInferred_thenOperationsEmpty() {
    var scriptFiles = scriptFiles(null, List.of(), List.of());
    var loader = new GraphqlSourceLoader(scriptFiles, resolver);

    var result = loader.createInferredApiSources("type Query { foo: String }");

    assertThat(result.schema().getDefinition()).isEqualTo("type Query { foo: String }");
    assertThat(result.operations()).isEmpty();
  }

  @Test
  void givenMissingOperationFile_whenConstruct_thenThrowsIllegalArgument() {
    var scriptFiles = scriptFiles(null, List.of("does-not-exist.graphql"), List.of());

    assertThatThrownBy(() -> new GraphqlSourceLoader(scriptFiles, resolver))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("does-not-exist.graphql");
  }

  private static ScriptFiles scriptFiles(
      @Nullable String graphql, List<String> operations, List<ScriptApiConfig> apiConfigs) {
    return new ScriptFiles(stubPackageJson(graphql, operations, apiConfigs));
  }

  private static PackageJson stubPackageJson(
      @Nullable String graphql, List<String> operations, List<ScriptApiConfig> apiConfigs) {
    var scriptConfig =
        new ScriptConfig() {
          @Override
          public Optional<String> getMainScript() {
            return Optional.empty();
          }

          @Override
          public List<ScriptApiConfig> getScriptApiConfigs() {
            return apiConfigs;
          }

          @Override
          public Optional<String> getGraphql() {
            return Optional.ofNullable(graphql);
          }

          @Override
          public List<String> getOperations() {
            return operations;
          }

          @Override
          public Map<String, Object> getConfig() {
            return Map.of();
          }

          @Override
          public Optional<String> getDatabase() {
            return Optional.empty();
          }

          @Override
          public void setMainScript(String script) {}

          @Override
          public void setGraphql(String g) {}
        };

    return new PackageJson() {
      @Override
      public List<String> getEnabledEngines() {
        return List.of();
      }

      @Override
      public void setEnabledEngines(List<String> enabledEngines) {}

      @Override
      public EnginesConfig getEngines() {
        return null;
      }

      @Override
      public ConnectorsConfig getConnectors() {
        return null;
      }

      @Override
      public DiscoveryConfig getDiscovery() {
        return null;
      }

      @Override
      public void toFile(Path path, boolean pretty) {}

      @Override
      public ScriptConfig getScriptConfig() {
        return scriptConfig;
      }

      @Override
      public CompilerConfig getCompilerConfig() {
        return null;
      }

      @Override
      public int getVersion() {
        return 1;
      }

      @Override
      public boolean hasScriptKey() {
        return true;
      }

      @Override
      public TestRunnerConfiguration getTestConfig() {
        return null;
      }
    };
  }

  private static ScriptApiConfig apiConfig(List<String> operations) {
    return new ScriptApiConfig() {
      @Override
      public String getVersion() {
        return "v1";
      }

      @Override
      public String getSchema() {
        return "v1-schema.graphqls";
      }

      @Override
      public List<String> getOperations() {
        return operations;
      }
    };
  }
}
