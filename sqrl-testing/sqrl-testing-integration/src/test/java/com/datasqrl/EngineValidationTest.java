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
import static org.assertj.core.api.Assertions.assertThat;

import com.datasqrl.SnapshotTestSupport.TestNameModifier;
import com.datasqrl.util.ArgumentsProviders;
import com.datasqrl.util.SnapshotTest.Snapshot;
import java.nio.file.Path;
import java.util.function.Predicate;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

class EngineValidationTest {

  public static final Path PROJECT_DIR = getResourcesDirectory("engine-validation");

  @RegisterExtension
  final CliCompileTestExtension snapshotExtension =
      new CliCompileTestExtension(Path.of("plan-output"));

  @ParameterizedTest
  @ArgumentsSource(ProjectCaseFiles.class)
  void testEngineValidation(Path packageFile) {
    assertThat(packageFile).isRegularFile();

    var testModifier = TestNameModifier.of(packageFile);
    var expectFailure = testModifier == TestNameModifier.fail;
    var printMessages =
        testModifier == TestNameModifier.fail || testModifier == TestNameModifier.warn;
    snapshotExtension.setSnapshot(Snapshot.of(getDisplayName(packageFile), getClass()));
    var hook =
        snapshotExtension.execute(
            PROJECT_DIR,
            "compile",
            packageFile.getFileName().toString(),
            "-t",
            snapshotExtension.getOutputDir().toAbsolutePath().toString());
    assertThat(hook.isFailed()).as(hook.getMessages()).isEqualTo(expectFailure);
    if (printMessages) {
      snapshotExtension.createMessageSnapshot(hook.getMessages());
    } else {
      snapshotExtension.createSnapshot(buildDir -> false, getOutputDirFilter(), planDir -> true);
    }
  }

  private Predicate<Path> getOutputDirFilter() {
    return path ->
        path.getFileName().toString().contains("iceberg")
            || path.getFileName().toString().equals("vertx.json");
  }

  static class ProjectCaseFiles extends ArgumentsProviders.PackageProvider {
    ProjectCaseFiles() {
      super(PROJECT_DIR);
    }
  }
}
