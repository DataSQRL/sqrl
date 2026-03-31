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

import static com.datasqrl.SnapshotTestSupport.getNestedDisplayName;
import static org.assertj.core.api.Assertions.assertThat;

import com.datasqrl.cli.AssertStatusHook;
import com.datasqrl.util.SnapshotTest.Snapshot;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import javax.annotation.Nullable;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * Static helper methods for use-case compile tests. Replaces the former {@code AbstractUseCaseTest}
 * inheritance hierarchy with composition.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class UseCaseTestHelper {

  /** Default build-dir filter for use-case compile tests. */
  public static Predicate<Path> defaultBuildDirFilter() {
    return file ->
        file.getFileName().toString().equals("pipeline_explain.txt")
            || file.getFileName().toString().equals("inferred_schema.graphqls");
  }

  /** Default plan-dir filter for use-case compile tests. */
  public static Predicate<Path> defaultPlanDirFilter() {
    return path -> {
      var fileName = path.getFileName().toString();
      if (fileName.startsWith("flink-sql-no-functions")) {
        return true;
      }

      if (fileName.contains("flink")) {
        return false;
      }

      return fileName.contains("schema")
          || fileName.contains("views")
          || List.of("kafka.json", "vertx.json").contains(fileName);
    };
  }

  /** Compiles the given package file and creates/validates a snapshot of the results. */
  public static void testUseCase(
      SnapshotDirectoryExtension ext,
      Class<?> testClass,
      Path packageFile,
      Predicate<Path> buildDirFilter,
      Predicate<Path> planDirFilter) {
    testUseCase(ext, testClass, packageFile, null, buildDirFilter, planDirFilter);
  }

  /**
   * Compiles the given package file and creates/validates a snapshot of the results. If {@code
   * testName} is null, one is derived from the package file path.
   */
  public static void testUseCase(
      SnapshotDirectoryExtension ext,
      Class<?> testClass,
      Path packageFile,
      @Nullable String testName,
      Predicate<Path> buildDirFilter,
      Predicate<Path> planDirFilter) {
    assertThat(packageFile).isRegularFile();

    var baseDir = packageFile.getParent();
    var args = new ArrayList<String>();
    args.add("compile");
    args.add(packageFile.getFileName().toString());

    var hook = ext.execute(baseDir, args);

    if (testName == null) {
      var useCasesPath = packageFile.getParent();
      while (useCasesPath.getFileName() != null
          && !useCasesPath.getFileName().toString().equals("usecases")) {
        useCasesPath = useCasesPath.getParent();
      }
      // If we are not running this under "usecases", we assume parent directory
      if (useCasesPath.getFileName() == null) useCasesPath = packageFile.getParent();

      var displayPath = useCasesPath.relativize(packageFile);
      testName = getNestedDisplayName(displayPath);
    }

    snapshot(ext, testClass, testName, hook, buildDirFilter, planDirFilter);
  }

  /**
   * Either snapshot the results in the plan and build directory (if successful) or the error
   * message (if it failed).
   */
  public static void snapshot(
      SnapshotDirectoryExtension ext,
      Class<?> testClass,
      String testname,
      AssertStatusHook hook,
      Predicate<Path> buildDirFilter,
      Predicate<Path> planDirFilter) {
    ext.setSnapshot(Snapshot.of(testname, testClass));
    if (hook.isFailed()) {
      ext.createMessageSnapshot(hook.getMessages());
    } else {
      ext.createSnapshot(buildDirFilter, path -> false, planDirFilter);
    }
  }
}
