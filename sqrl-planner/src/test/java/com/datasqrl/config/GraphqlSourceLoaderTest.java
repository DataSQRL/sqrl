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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datasqrl.config.PackageJson.ScriptApiConfig;
import com.datasqrl.engine.server.ServerPhysicalPlan;
import com.datasqrl.error.CollectedException;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.plan.validate.ExecutionGoal;
import com.datasqrl.server.ApiSource;
import com.datasqrl.server.GraphqlSchemaHandler;
import com.datasqrl.server.ScriptFiles;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class GraphqlSourceLoaderTest {

  @TempDir Path tempDir;

  @Mock ScriptFiles scriptFiles;
  @Mock GraphqlSchemaHandler graphqlSchemaHandler;
  @Mock ServerPhysicalPlan serverPlan;

  private final PackageJson config = createConfig(true);

  private static PackageJson createConfig(boolean useInferredSchema) {
    var sqrlConfig = SqrlConfig.createCurrentVersion();
    sqrlConfig.setProperty("test-runner.use-inferred-schema", useInferredSchema);
    return new PackageJsonImpl(sqrlConfig);
  }

  @Test
  void givenExplicitSchemaAndOperations_whenLoad_thenValidatesAndReturnsExplicitVersions()
      throws IOException {
    writeFile("schema.graphqls", "type Query { foo: String }");
    writeFile("op.graphql", "query Foo { foo }");
    when(scriptFiles.getApiConfigs()).thenReturn(List.of());
    when(scriptFiles.getGraphql()).thenReturn(Optional.of("schema.graphqls"));
    when(scriptFiles.getOperations()).thenReturn(List.of("op.graphql"));

    var loader =
        new GraphqlSourceLoader(
            scriptFiles,
            workspacePaths(),
            graphqlSchemaHandler,
            config,
            ExecutionGoal.COMPILE,
            ErrorCollector.root());
    var result = loader.load(serverPlan);

    assertThat(result.apiVersions()).hasSize(1);
    assertThat(result.apiVersions().get(0).schema().getDefinition())
        .isEqualTo("type Query { foo: String }");
    assertThat(result.apiVersions().get(0).operations())
        .extracting(ApiSource::getDefinition)
        .containsExactly("query Foo { foo }");
    assertThat(result.inferredSchema()).isEmpty();
    verify(graphqlSchemaHandler).validateSchema(result.apiVersions().get(0), serverPlan);
    verify(graphqlSchemaHandler, never()).inferGraphQLSchema(any());
  }

  @Test
  void givenApiConfigs_whenLoad_thenValidatesConfiguredVersions() throws IOException {
    writeFile("v1-schema.graphqls", "type Query { foo: String }");
    when(scriptFiles.getApiConfigs())
        .thenReturn(
            List.of(
                new ScriptApiConfig() {
                  @Override
                  public String getVersion() {
                    return "v1";
                  }

                  @Override
                  public Optional<String> getSchema() {
                    return Optional.of("v1-schema.graphqls");
                  }
                }));

    var loader =
        new GraphqlSourceLoader(
            scriptFiles,
            workspacePaths(),
            graphqlSchemaHandler,
            config,
            ExecutionGoal.COMPILE,
            ErrorCollector.root());
    var result = loader.load(serverPlan);

    assertThat(result.apiVersions()).hasSize(1);
    assertThat(result.apiVersions().get(0).version()).isEqualTo("v1");
    assertThat(result.inferredSchema()).isEmpty();
    verify(graphqlSchemaHandler).validateSchema(result.apiVersions().get(0), serverPlan);
  }

  @Test
  void givenVersionedOpenApiWithoutSchema_whenLoad_thenInfersVersionedSchema() throws IOException {
    writeFile("op.graphql", "query Foo { foo }");
    when(scriptFiles.getApiConfigs())
        .thenReturn(
            List.of(
                new ScriptApiConfig() {
                  @Override
                  public String getVersion() {
                    return "v2";
                  }

                  @Override
                  public List<String> getOperations() {
                    return List.of("op.graphql");
                  }

                  @Override
                  public Optional<String> getOpenApi() {
                    return Optional.of("openapi.json");
                  }
                }));
    when(graphqlSchemaHandler.inferGraphQLSchema(serverPlan))
        .thenReturn("type Query { foo: String }");

    var loader =
        new GraphqlSourceLoader(
            scriptFiles,
            workspacePaths(),
            graphqlSchemaHandler,
            config,
            ExecutionGoal.COMPILE,
            ErrorCollector.root());
    var result = loader.load(serverPlan);

    assertThat(result.inferredSchema()).contains("type Query { foo: String }");
    assertThat(result.apiVersions())
        .singleElement()
        .satisfies(
            api -> {
              assertThat(api.version()).isEqualTo("v2");
              assertThat(api.schema().getDefinition()).isEqualTo("type Query { foo: String }");
              assertThat(api.schema().getPath()).isEmpty();
              assertThat(api.operations())
                  .extracting(ApiSource::getDefinition)
                  .containsExactly("query Foo { foo }");
            });
    verify(graphqlSchemaHandler, never()).validateSchema(any(), any());
  }

  @Test
  void givenVersionedOperationsWithoutSchema_whenLoad_thenInfersVersionedSchema()
      throws IOException {
    writeFile("op.graphql", "query Foo { foo }");
    when(scriptFiles.getApiConfigs())
        .thenReturn(
            List.of(
                new ScriptApiConfig() {
                  @Override
                  public String getVersion() {
                    return "v2";
                  }

                  @Override
                  public List<String> getOperations() {
                    return List.of("op.graphql");
                  }
                }));
    when(graphqlSchemaHandler.inferGraphQLSchema(serverPlan))
        .thenReturn("type Query { foo: String }");

    var loader =
        new GraphqlSourceLoader(
            scriptFiles,
            workspacePaths(),
            graphqlSchemaHandler,
            config,
            ExecutionGoal.COMPILE,
            ErrorCollector.root());
    var result = loader.load(serverPlan);

    assertThat(result.inferredSchema()).contains("type Query { foo: String }");
    assertThat(result.apiVersions())
        .singleElement()
        .satisfies(
            api -> {
              assertThat(api.version()).isEqualTo("v2");
              assertThat(api.schema().getDefinition()).isEqualTo("type Query { foo: String }");
              assertThat(api.operations())
                  .extracting(ApiSource::getDefinition)
                  .containsExactly("query Foo { foo }");
            });
    verify(graphqlSchemaHandler, never()).validateSchema(any(), any());
  }

  @Test
  void givenNoSchemaWithOperations_whenLoad_thenInfersSchemaAndIncludesOperations()
      throws IOException {
    writeFile("op.graphql", "query Foo { foo }");
    when(scriptFiles.getApiConfigs()).thenReturn(List.of());
    when(scriptFiles.getGraphql()).thenReturn(Optional.empty());
    when(scriptFiles.getOperations()).thenReturn(List.of("op.graphql"));
    when(graphqlSchemaHandler.inferGraphQLSchema(serverPlan))
        .thenReturn("type Query { foo: String }");

    var loader =
        new GraphqlSourceLoader(
            scriptFiles,
            workspacePaths(),
            graphqlSchemaHandler,
            config,
            ExecutionGoal.COMPILE,
            ErrorCollector.root());
    var result = loader.load(serverPlan);

    assertThat(result.inferredSchema()).contains("type Query { foo: String }");
    assertThat(result.apiVersions()).hasSize(1);
    assertThat(result.apiVersions().get(0).operations())
        .extracting(ApiSource::getDefinition)
        .containsExactly("query Foo { foo }");
    verify(graphqlSchemaHandler, never()).validateSchema(any(), any());
  }

  @Test
  void givenNoSchemaNoOperations_whenLoad_thenInfersSchemaWithEmptyOperations() {
    when(scriptFiles.getApiConfigs()).thenReturn(List.of());
    when(scriptFiles.getGraphql()).thenReturn(Optional.empty());
    when(scriptFiles.getOperations()).thenReturn(List.of());
    when(graphqlSchemaHandler.inferGraphQLSchema(serverPlan))
        .thenReturn("type Query { foo: String }");

    var loader =
        new GraphqlSourceLoader(
            scriptFiles,
            workspacePaths(),
            graphqlSchemaHandler,
            config,
            ExecutionGoal.COMPILE,
            ErrorCollector.root());
    var result = loader.load(serverPlan);

    assertThat(result.inferredSchema()).contains("type Query { foo: String }");
    assertThat(result.apiVersions()).hasSize(1);
    assertThat(result.apiVersions().get(0).operations()).isEmpty();
  }

  @Test
  void givenExplicitSchemaInTestMode_whenUseInferredSchema_thenOverridesWithInferred()
      throws IOException {
    writeFile("schema.graphqls", "type Query { foo: String }");
    writeFile("op.graphql", "query Foo { foo }");
    when(scriptFiles.getApiConfigs()).thenReturn(List.of());
    when(scriptFiles.getGraphql()).thenReturn(Optional.of("schema.graphqls"));
    when(scriptFiles.getOperations()).thenReturn(List.of("op.graphql"));
    when(graphqlSchemaHandler.inferGraphQLSchema(serverPlan))
        .thenReturn("type Query { bar: String }");

    var loader =
        new GraphqlSourceLoader(
            scriptFiles,
            workspacePaths(),
            graphqlSchemaHandler,
            config,
            ExecutionGoal.TEST,
            ErrorCollector.root());
    var result = loader.load(serverPlan);

    assertThat(result.inferredSchema()).contains("type Query { bar: String }");
    assertThat(result.apiVersions()).hasSize(1);
    assertThat(result.apiVersions().get(0).schema().getDefinition())
        .isEqualTo("type Query { bar: String }");
    assertThat(result.apiVersions().get(0).operations())
        .extracting(ApiSource::getDefinition)
        .containsExactly("query Foo { foo }");
    verify(graphqlSchemaHandler, never()).validateSchema(any(), any());
  }

  @Test
  void givenMissingVersionedSchemaInTestMode_whenLoad_thenThrowsIllegalArgument() {
    when(scriptFiles.getApiConfigs())
        .thenReturn(
            List.of(
                new ScriptApiConfig() {
                  @Override
                  public String getVersion() {
                    return "v1";
                  }

                  @Override
                  public Optional<String> getSchema() {
                    return Optional.of("does-not-exist.graphqls");
                  }
                }));

    var loader =
        new GraphqlSourceLoader(
            scriptFiles,
            workspacePaths(),
            graphqlSchemaHandler,
            config,
            ExecutionGoal.TEST,
            ErrorCollector.root());

    assertThatThrownBy(() -> loader.load(serverPlan))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("does-not-exist.graphqls");
  }

  @Test
  void givenMissingFile_whenLoad_thenThrowsIllegalArgument() {
    when(scriptFiles.getApiConfigs()).thenReturn(List.of());
    when(scriptFiles.getGraphql()).thenReturn(Optional.empty());
    when(scriptFiles.getOperations()).thenReturn(List.of("does-not-exist.graphql"));

    var loader =
        new GraphqlSourceLoader(
            scriptFiles,
            workspacePaths(),
            graphqlSchemaHandler,
            config,
            ExecutionGoal.COMPILE,
            ErrorCollector.root());

    assertThatThrownBy(() -> loader.load(serverPlan))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("does-not-exist.graphql");
  }

  @Test
  void givenSchemaOfSiblingProjectInWorkspace_whenLoad_thenReadsItWhereItIs() throws IOException {
    var projectDir = Files.createDirectories(tempDir.resolve("project"));
    writeFile("shared/schema.graphqls", "type Query { shared: String }");
    when(scriptFiles.getApiConfigs()).thenReturn(List.of(apiConfig("../shared/schema.graphqls")));

    var loader =
        new GraphqlSourceLoader(
            scriptFiles,
            new WorkspacePaths(tempDir, projectDir.resolve("build"), tempDir, projectDir),
            graphqlSchemaHandler,
            config,
            ExecutionGoal.COMPILE,
            ErrorCollector.root());
    var result = loader.load(serverPlan);

    assertThat(result.apiVersions().get(0).schema().getDefinition())
        .isEqualTo("type Query { shared: String }");
  }

  @Test
  void givenSchemaOutsideWorkspace_whenLoad_thenFailsNamingKeyAndValue() {
    when(scriptFiles.getApiConfigs()).thenReturn(List.of(apiConfig("../outside.graphqls")));

    var loader =
        new GraphqlSourceLoader(
            scriptFiles,
            workspacePaths(),
            graphqlSchemaHandler,
            config,
            ExecutionGoal.COMPILE,
            ErrorCollector.root());

    assertThatThrownBy(() -> loader.load(serverPlan))
        .isInstanceOf(CollectedException.class)
        .hasMessage(
            "Value [../outside.graphqls] for key [script.api.v1.schema] is not valid. "
                + ProjectPaths.RULE);
  }

  private static ScriptApiConfig apiConfig(String schema) {
    return new ScriptApiConfig() {
      @Override
      public String getVersion() {
        return "v1";
      }

      @Override
      public Optional<String> getSchema() {
        return Optional.of(schema);
      }
    };
  }

  /** The temporary directory is the workspace, the project root and the build directory. */
  private WorkspacePaths workspacePaths() {
    return new WorkspacePaths(tempDir, tempDir, tempDir.resolve("deploy"), tempDir);
  }

  private Path writeFile(String name, String content) throws IOException {
    var path = tempDir.resolve(name);
    Files.createDirectories(path.getParent());
    Files.writeString(path, content);
    return path;
  }
}
