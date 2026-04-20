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
import com.datasqrl.graphql.ApiSource;
import com.datasqrl.graphql.GraphqlSchemaHandler;
import com.datasqrl.graphql.ScriptFiles;
import com.datasqrl.loaders.resolver.ResourceResolver;
import com.datasqrl.plan.validate.ExecutionGoal;
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
  @Mock ResourceResolver resolver;
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
    var schemaPath = writeFile("schema.graphqls", "type Query { foo: String }");
    var opPath = writeFile("op.graphql", "query Foo { foo }");
    when(scriptFiles.getApiConfigs()).thenReturn(List.of());
    when(scriptFiles.getGraphql()).thenReturn(Optional.of("schema.graphqls"));
    when(scriptFiles.getOperations()).thenReturn(List.of("op.graphql"));
    when(resolver.resolveFile(Path.of("schema.graphqls"))).thenReturn(Optional.of(schemaPath));
    when(resolver.resolveFile(Path.of("op.graphql"))).thenReturn(Optional.of(opPath));

    var loader =
        new GraphqlSourceLoader(
            scriptFiles, resolver, graphqlSchemaHandler, config, ExecutionGoal.COMPILE);
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
    var schemaPath = writeFile("v1-schema.graphqls", "type Query { foo: String }");
    when(scriptFiles.getApiConfigs())
        .thenReturn(
            List.of(
                new ScriptApiConfig() {
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
                    return List.of();
                  }
                }));
    when(resolver.resolveFile(Path.of("v1-schema.graphqls"))).thenReturn(Optional.of(schemaPath));

    var loader =
        new GraphqlSourceLoader(
            scriptFiles, resolver, graphqlSchemaHandler, config, ExecutionGoal.COMPILE);
    var result = loader.load(serverPlan);

    assertThat(result.apiVersions()).hasSize(1);
    assertThat(result.apiVersions().get(0).version()).isEqualTo("v1");
    assertThat(result.inferredSchema()).isEmpty();
    verify(graphqlSchemaHandler).validateSchema(result.apiVersions().get(0), serverPlan);
  }

  @Test
  void givenNoSchemaWithOperations_whenLoad_thenInfersSchemaAndIncludesOperations()
      throws IOException {
    var opPath = writeFile("op.graphql", "query Foo { foo }");
    when(scriptFiles.getApiConfigs()).thenReturn(List.of());
    when(scriptFiles.getGraphql()).thenReturn(Optional.empty());
    when(scriptFiles.getOperations()).thenReturn(List.of("op.graphql"));
    when(resolver.resolveFile(Path.of("op.graphql"))).thenReturn(Optional.of(opPath));
    when(graphqlSchemaHandler.inferGraphQLSchema(serverPlan))
        .thenReturn("type Query { foo: String }");

    var loader =
        new GraphqlSourceLoader(
            scriptFiles, resolver, graphqlSchemaHandler, config, ExecutionGoal.COMPILE);
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
            scriptFiles, resolver, graphqlSchemaHandler, config, ExecutionGoal.COMPILE);
    var result = loader.load(serverPlan);

    assertThat(result.inferredSchema()).contains("type Query { foo: String }");
    assertThat(result.apiVersions()).hasSize(1);
    assertThat(result.apiVersions().get(0).operations()).isEmpty();
  }

  @Test
  void givenExplicitSchemaInTestMode_whenUseInferredSchema_thenOverridesWithInferred()
      throws IOException {
    var schemaPath = writeFile("schema.graphqls", "type Query { foo: String }");
    var opPath = writeFile("op.graphql", "query Foo { foo }");
    when(scriptFiles.getApiConfigs()).thenReturn(List.of());
    when(scriptFiles.getGraphql()).thenReturn(Optional.of("schema.graphqls"));
    when(scriptFiles.getOperations()).thenReturn(List.of("op.graphql"));
    when(resolver.resolveFile(Path.of("schema.graphqls"))).thenReturn(Optional.of(schemaPath));
    when(resolver.resolveFile(Path.of("op.graphql"))).thenReturn(Optional.of(opPath));
    when(graphqlSchemaHandler.inferGraphQLSchema(serverPlan))
        .thenReturn("type Query { bar: String }");

    var loader =
        new GraphqlSourceLoader(
            scriptFiles, resolver, graphqlSchemaHandler, config, ExecutionGoal.TEST);
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
  void givenMissingFile_whenLoad_thenThrowsIllegalArgument() {
    when(scriptFiles.getApiConfigs()).thenReturn(List.of());
    when(scriptFiles.getGraphql()).thenReturn(Optional.empty());
    when(scriptFiles.getOperations()).thenReturn(List.of("does-not-exist.graphql"));
    when(resolver.resolveFile(Path.of("does-not-exist.graphql"))).thenReturn(Optional.empty());

    var loader =
        new GraphqlSourceLoader(
            scriptFiles, resolver, graphqlSchemaHandler, config, ExecutionGoal.COMPILE);

    assertThatThrownBy(() -> loader.load(serverPlan))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("does-not-exist.graphql");
  }

  private Path writeFile(String name, String content) throws IOException {
    var path = tempDir.resolve(name);
    Files.writeString(path, content);
    return path;
  }
}
