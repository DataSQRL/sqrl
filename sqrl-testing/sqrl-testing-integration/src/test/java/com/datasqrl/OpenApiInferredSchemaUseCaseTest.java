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
package com.datasqrl;

import static com.datasqrl.SnapshotTestSupport.getResourcesDirectory;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class OpenApiInferredSchemaUseCaseTest {

  private static final Path USE_CASE_DIR =
      getResourcesDirectory("usecases/openapi-inferred-schema-compile");

  @RegisterExtension final CliCompileTestExtension compileExtension = new CliCompileTestExtension();

  @Test
  void givenOpenApiOnlyVersionedApi_whenCompile_thenGeneratesVersionedOpenApiArtifact()
      throws IOException {
    var status = compileExtension.execute(USE_CASE_DIR, List.of("compile", "package.json"));

    assertThat(status.isSuccess()).isTrue();
    var openApiArtifact = compileExtension.getPlanDir().resolve("vertx-v2-openapi.json");
    assertThat(openApiArtifact).isRegularFile();
    assertThat(Files.readString(openApiArtifact)).contains("\"openapi\" : \"3.0.1\"");
  }

  @Test
  void givenIncompatibleOpenApiBaseline_whenCompile_thenFailsCompatibilityCheck() {
    var status =
        compileExtension.execute(
            USE_CASE_DIR, List.of("compile", "package-inferred-schema-incompatible.json"));

    assertThat(status.isFailed()).isTrue();
    assertThat(status.getMessages())
        .contains("not backwards compatible")
        .contains("openapi-incompatible.json");
  }

  @Test
  void givenVersionedOperationsWithoutSchema_whenCompile_thenGeneratesOperationsOnlyOpenApi()
      throws IOException {
    var status =
        compileExtension.execute(
            USE_CASE_DIR, List.of("compile", "package-operations-inferred-schema.json"));

    assertThat(status.isSuccess()).isTrue();
    var openApiArtifact = compileExtension.getPlanDir().resolve("vertx-v3-openapi.json");
    assertThat(openApiArtifact).isRegularFile();
    assertThat(Files.readString(openApiArtifact)).contains("/v3/rest/queries/GetGreetingById");
  }
}
