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

import static com.datasqrl.SnapshotTestSupport.getDisplayName;
import static com.datasqrl.SnapshotTestSupport.getResourcesDirectory;

import com.datasqrl.compile.DagWriter;
import com.datasqrl.util.ArgumentsProviders;
import java.nio.file.Path;
import java.util.function.Predicate;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

/**
 * Validates the schemas based on comprehensiveTest.sqrl script and snapshots the deployment assets
 */
public class GraphQLValidationTest {

  private static final Path USECASE_DIR = getResourcesDirectory("graphql-validation");

  @RegisterExtension
  final SnapshotDirectoryExtension snapshotExtension = new SnapshotDirectoryExtension();

  @ParameterizedTest
  @ArgumentsSource(GraphQLSchemas.class)
  void testUseCase(Path graphQLSchema) {
    snapshotExtension.writeTempPackage(graphQLSchema, "__GRAPHQL_SCHEMA__");

    UseCaseTestHelper.testUseCase(
        snapshotExtension,
        getClass(),
        snapshotExtension.getTempPackage(),
        getDisplayName(graphQLSchema),
        getBuildDirFilter(),
        UseCaseTestHelper.defaultPlanDirFilter());
  }

  private Predicate<Path> getBuildDirFilter() {
    return file -> {
      var fileName = file.getFileName().toString();
      return fileName.equals(DagWriter.EXPLAIN_TEXT_FILENAME) || fileName.endsWith(".graphqls");
    };
  }

  static class GraphQLSchemas extends ArgumentsProviders.GraphQLSchemaProvider {
    public GraphQLSchemas() {
      super(USECASE_DIR, true);
    }
  }
}
