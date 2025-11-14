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
import static org.assertj.core.api.Assumptions.assumeThat;

import com.datasqrl.util.SnapshotTest.Snapshot;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Predicate;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

/**
 * Parametrized Test for parsing and planning of SQRL scripts in resources/dagplanner. Add SQRL
 * scripts to this folder to test the parser and planner. This test executes quickly.
 *
 * <p>This test snapshots the produced DAG plan and deployment assets. SQRL scripts with names
 * ending in `-fail` are expected to produce errors which are snapshotted. SQRL scripts ending in
 * `-warn` are expected to produce warnings which are snapshotted. SQRL scripts ending in -disabled`
 * are ignored.
 */
public class DAGPlannerTest extends AbstractAssetSnapshotTest {

  public static final Path SCRIPT_DIR = getResourcesDirectory("dagplanner");

  protected DAGPlannerTest() {
    super(SCRIPT_DIR.resolve("plan-output"));
  }

  @ParameterizedTest
  @ArgumentsSource(DagPlannerSQRLFiles.class)
  void scripts(Path script) {
    assertThat(Files.exists(script)).isTrue();
    var testModifier = TestNameModifier.of(script);
    var expectFailure = testModifier == TestNameModifier.fail;
    var printMessages =
        testModifier == TestNameModifier.fail || testModifier == TestNameModifier.warn;
    this.snapshot = Snapshot.of(getDisplayName(script), getClass());
    var hook =
        execute(
            SCRIPT_DIR,
            "compile",
            script.getFileName().toString(),
            "-t",
            outputDir.getFileName().toString());
    assertThat(hook.isFailed()).as(hook.getMessages()).isEqualTo(expectFailure);
    if (printMessages) {
      createMessageSnapshot(hook.getMessages());
    } else {
      createSnapshot();
    }
  }

  @ParameterizedTest
  @ArgumentsSource(DagPlannerSQRLFiles.class)
  @Disabled
  void specificScript(Path script) {
    assumeThat(script.toString()).endsWith("functionParameterExpressionTest.sqrl");
    scripts(script);
  }

  @Override
  public Predicate<Path> getBuildDirFilter() {
    return file -> {
      switch (file.getFileName().toString()) {
        case "pipeline_explain.txt":
          return true;
      }
      return false;
    };
  }

  @Override
  public Predicate<Path> getOutputDirFilter() {
    return path -> {
      if (path.getFileName().toString().equals("flink-sql-no-functions.sql")) {
        return true;
      }
      if (path.getFileName().toString().contains("flink")
          || path.getFileName().toString().contains("schema")
          || path.getFileName().toString().contains("views")
          || path.getFileName().toString().endsWith("ser")
          || path.getFileName().toString().startsWith("vertx-config.json")) {
        return false;
      }
      return true;
    };
  }

  static class DagPlannerSQRLFiles extends SqrlScriptArgumentsProvider {
    public DagPlannerSQRLFiles() {
      super(SCRIPT_DIR, true);
    }
  }
}
