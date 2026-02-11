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

import static org.assertj.core.api.Assertions.assertThat;

import com.datasqrl.cli.AssertStatusHook;
import com.datasqrl.util.SnapshotTest.Snapshot;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import javax.annotation.Nullable;

public class AbstractUseCaseTest extends AbstractAssetSnapshotTest {

  protected AbstractUseCaseTest() {
    super(null);
  }

  void testUseCase(Path packageFile) {
    testUseCase(packageFile, null);
  }

  void testUseCase(Path packageFile, @Nullable String testName) {
    assertThat(packageFile).isRegularFile();

    var baseDir = packageFile.getParent();
    var args = new ArrayList<String>();
    args.add("compile");
    args.add(packageFile.getFileName().toString());

    var hook = execute(baseDir, args);

    if (testName == null) {
      var useCasesPath = packageFile.getParent();
      while (!useCasesPath.getFileName().toString().equals("usecases")) {
        useCasesPath = useCasesPath.getParent();
      }

      var displayPath = useCasesPath.relativize(packageFile);
      testName = getNestedDisplayName(displayPath);
    }

    snapshot(testName, hook);
  }

  /**
   * Either snapshot the results in the plan and build directory (if successful) or the error
   * message (if it failed)
   *
   * @param testname
   * @param hook
   */
  public void snapshot(String testname, AssertStatusHook hook) {
    this.snapshot = Snapshot.of(testname, getClass());
    if (hook.isFailed()) {
      createMessageSnapshot(hook.getMessages());
    } else {
      createSnapshot();
    }
  }

  @Override
  public Predicate<Path> getBuildDirFilter() {
    return file ->
        file.getFileName().toString().equals("pipeline_explain.txt")
            || file.getFileName().toString().equals("inferred_schema.graphqls");
  }

  @Override
  public Predicate<Path> getPlanDirFilter() {
    return path -> {
      var fileName = path.getFileName().toString();
      if (fileName.equals("flink-sql-no-functions.sql")) {
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
}
