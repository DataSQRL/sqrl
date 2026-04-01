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

import com.datasqrl.util.ArgumentsProviders;
import java.nio.file.Path;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
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
public class UseCaseCompileTest {

  private static final Path USECASE_DIR = getResourcesDirectory("usecases");

  @RegisterExtension
  final CliCompileTestExtension snapshotExtension = new CliCompileTestExtension();

  @ParameterizedTest
  @ArgumentsSource(UseCaseFiles.class)
  void testUseCase(Path packageFile) {
    UseCaseTestHelper.testUseCase(
        snapshotExtension,
        getClass(),
        packageFile,
        UseCaseTestHelper.defaultBuildDirFilter(),
        UseCaseTestHelper.defaultPlanDirFilter());
  }

  @Test
  @Disabled("Intended for manual usage")
  void runTestCaseByName() {
    var pkg = USECASE_DIR.resolve("multi-batch-compile").resolve("package.json");
    UseCaseTestHelper.testUseCase(
        snapshotExtension,
        getClass(),
        pkg,
        UseCaseTestHelper.defaultBuildDirFilter(),
        UseCaseTestHelper.defaultPlanDirFilter());
  }

  static class UseCaseFiles extends ArgumentsProviders.PackageProvider {
    UseCaseFiles() {
      super(USECASE_DIR);
    }
  }
}
