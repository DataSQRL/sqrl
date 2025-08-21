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

import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.nio.file.Path;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

/**
 * Parametrized Test for parsing and planning of SQRL scripts in resources/usecases. Add entire SQRL
 * projects to this folder to test the parser and planner. This test executes quickly.
 *
 * <p>This test snapshots the produced DAG plan and deployment assets. SQRL scripts with names
 * ending in `-fail` are expected to produce errors which are snapshotted. SQRL scripts ending in
 * `-warn` are expected to produce warnings which are snapshotted. SQRL scripts ending in -disabled`
 * are ignored.
 */
public class UseCaseCompileTest extends AbstractUseCaseTest {

  public static final Path USECASE_DIR = getResourcesDirectory("usecases");

  @Override
  @SneakyThrows
  @ParameterizedTest
  @ArgumentsSource(UseCaseFiles.class)
  void testUsecase(Path script, Path graphQlFile, Path packageFile) {
    super.testUsecase(script, graphQlFile, packageFile);
  }

  static class UseCaseFiles extends SqrlScriptsAndLocalPackages {
    public UseCaseFiles() {
      super(USECASE_DIR, true);
    }
  }

  @ParameterizedTest
  @ArgumentsSource(UseCaseFiles.class)
  @Disabled
  void runTestCaseByName(Path script, Path graphQlFile, Path packageFile) {
    assumeTrue(
        script.toString().endsWith("seedshop-extended.sqrl"), "Not the test marked for execution.");

    super.testUsecase(script, graphQlFile, packageFile);
  }
}
