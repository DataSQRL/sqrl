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

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class HintedIndexSelectionTest {

  private static final Path USECASE_DIR = getResourcesDirectory("usecases");

  @RegisterExtension
  final CliCompileTestExtension snapshotExtension = new CliCompileTestExtension();

  @Test
  void givenHintedTablesExceedIndexLimits_whenCompiled_thenRetainsHintedTableDependencies()
      throws Exception {
    var packageFile = USECASE_DIR.resolve("pg-index-selection-compile").resolve("package.json");
    var hook =
        snapshotExtension.execute(
            packageFile.getParent(), "compile", packageFile.getFileName().toString());

    assertThat(hook.isFailed()).as(hook.getMessages()).isFalse();
    assertThat(hook.getMessages()).doesNotContain("table `ExplicitIndexes`", "table `NoIndexes`");
    var plan = Files.readString(snapshotExtension.getBuildDir().resolve("pipeline_explain.txt"));
    assertEndpointInput(plan, "ExplicitByCarrierId", "ExplicitIndexes");
    assertEndpointInput(plan, "NoIndexByCarrierId", "NoIndexes");
    assertEndpointInput(plan, "ExplicitIndexesSink", "ExplicitIndexes");
  }

  private static void assertEndpointInput(String plan, String endpoint, String input) {
    var start = plan.indexOf("=== " + endpoint);
    var end = plan.indexOf("===", start + 3);
    assertThat(start).isGreaterThanOrEqualTo(0);
    assertThat(end).isGreaterThan(start);
    assertThat(plan.substring(start, end))
        .contains("Inputs:\n - default_catalog.default_database." + input);
  }
}
