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

import com.datasqrl.cli.AssertStatusHook;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * Compiles an old and a new version of a script, and runs the state upgrade check on the two
 * compiled Flink plans. The tests use the plans that Flink writes, so a renamed plan field or an
 * operator kind without a {@code state} field shows up here.
 */
class StateUpgradeCheckCompileTest {

  private static final Path PROJECT_DIR = getResourcesDirectory("state-upgrade-check");

  /** The text in the package template that the test replaces with the script file. */
  private static final String SCRIPT_FILE_TEMPLATE = "SCRIPT_FILE";

  /** The text in the package template that the test replaces with the state TTL. */
  private static final String STATE_TTL_TEMPLATE = "STATE_TTL";

  /** A state TTL of 0 keeps the state without a time limit, as the Flink default. */
  private static final String NO_STATE_TTL = "0 ms";

  /** The text in the package template that the test replaces with the operator uid format. */
  private static final String UID_FORMAT_TEMPLATE = "UID_FORMAT";

  private static final String DEFAULT_UID_FORMAT =
      ExecutionConfigOptions.TABLE_EXEC_UID_FORMAT.defaultValue();

  /**
   * The {@code -b} values of the two compiles. {@code -b} resolves against the build folder, which
   * the extension deletes after each test.
   */
  private static final String OLD_BUILD_FOLDER = "old";

  private static final String NEW_BUILD_FOLDER = "new";

  /** The old build for {@code --check-state-upgrade-from}, which resolves against the project. */
  private static final String OLD_BUILD_FROM_PROJECT = "build/" + OLD_BUILD_FOLDER;

  @RegisterExtension final CliCompileTestExtension compileExtension = new CliCompileTestExtension();

  @Test
  void givenSameScript_whenCheckStateUpgrade_thenPasses() {
    var status = compileAndCheckUpgrade("window-count.sqrl", "window-count.sqrl");

    assertThat(status.isSuccess()).as(status.getMessages()).isTrue();
    assertThat(status.getMessages()).contains("The new Flink plan passes the state upgrade check");
  }

  @Test
  void givenNewAggregateOnWindow_whenCheckStateUpgrade_thenIncompatibleWindowState() {
    var status = compileAndCheckUpgrade("window-count.sqrl", "window-count-with-max.sqrl");

    assertThat(status.isSuccess()).as(status.getMessages()).isFalse();
    assertThat(status.getMessages())
        .containsPattern(
            "field 'outputType' differs: .*GlobalWindowAggregate.*\\[incompatible state]");
  }

  @Test
  void givenNewColumnOnIntervalJoin_whenCheckStateUpgrade_thenIncompatibleJoinState() {
    var status = compileAndCheckUpgrade("interval-join.sqrl", "interval-join-with-note.sqrl");

    assertThat(status.isSuccess()).as(status.getMessages()).isFalse();
    assertThat(status.getMessages())
        .containsPattern("field 'outputType' differs: .*IntervalJoin.*\\[incompatible state]");
  }

  @Test
  void givenNewTableMovesWindow_whenCheckStateUpgrade_thenUnclaimedWindowState() {
    var status = compileAndCheckUpgrade("window-count.sqrl", "window-count-with-new-table.sqrl");

    assertThat(status.isSuccess()).as(status.getMessages()).isFalse();
    assertThat(status.getMessages())
        .containsPattern(
            "field 'type' differs: stream-exec-global-window-aggregate_\\d+ -> .*\\[unclaimed"
                + " state]");
  }

  @Test
  void givenNewColumnOnDeduplicate_whenCheckStateUpgrade_thenIncompatibleDeduplicateState() {
    var status =
        compileAndCheckUpgrade("latest-event-total.sqrl", "latest-event-total-with-customer.sqrl");

    assertThat(status.isSuccess()).as(status.getMessages()).isFalse();
    assertThat(status.getMessages())
        .containsPattern("field 'outputType' differs: .*Deduplicate.*\\[incompatible state]");
  }

  @Test
  void givenChangedAggregateFunction_whenCheckStateUpgrade_thenIncompatibleAggregateState() {
    var status = compileAndCheckUpgrade("latest-event-total.sqrl", "latest-event-max.sqrl");

    assertThat(status.isSuccess()).as(status.getMessages()).isFalse();
    assertThat(status.getMessages())
        .containsPattern(
            "field 'aggCalls' differs: \\[SUM] -> \\[MAX] .*GroupAggregate.*\\[incompatible"
                + " state]");
  }

  @Test
  void givenRenamedColumn_whenCheckStateUpgrade_thenPasses() {
    var status =
        compileAndCheckUpgrade("latest-event-total.sqrl", "latest-event-total-renamed.sqrl");

    assertThat(status.isSuccess()).as(status.getMessages()).isTrue();
    assertThat(status.getMessages()).contains("The new Flink plan passes the state upgrade check");
  }

  @Test
  void givenChangedStateTtl_whenCheckStateUpgrade_thenPassesWithTtlNotice() {
    var status =
        compileAndCheckUpgrade(
            "latest-event-total.sqrl", NO_STATE_TTL, "latest-event-total.sqrl", "1 h");

    assertThat(status.isSuccess()).as(status.getMessages()).isTrue();
    assertThat(status.getMessages())
        .contains("the TTL of state 'deduplicateState' changes: 0 ms -> ")
        .contains("The new Flink plan passes the state upgrade check");
  }

  @Test
  void givenRemovedTable_whenCheckStateUpgrade_thenUnclaimedState() {
    var status =
        compileAndCheckUpgrade("window-count-and-latest-event-total.sqrl", "window-count.sqrl");

    assertThat(status.isSuccess()).as(status.getMessages()).isFalse();
    assertThat(status.getMessages())
        .containsPattern("removed: Deduplicate.*\\[unclaimed state]")
        .doesNotContain("GlobalWindowAggregate");
  }

  @Test
  void givenNewTableAfterOldTables_whenCheckStateUpgrade_thenPasses() {
    var status = compileAndCheckUpgrade("window-count.sqrl", "window-count-with-later-table.sqrl");

    assertThat(status.isSuccess()).as(status.getMessages()).isTrue();
    assertThat(status.getMessages()).contains("The new Flink plan passes the state upgrade check");
  }

  @Test
  void givenNewColumnOnTumbleTableWithoutAggregate_whenCheckStateUpgrade_thenPasses() {
    var status = compileAndCheckUpgrade("window-table.sqrl", "window-table-with-note.sqrl");

    assertThat(status.isSuccess()).as(status.getMessages()).isTrue();
    assertThat(status.getMessages()).contains("The new Flink plan passes the state upgrade check");
    assertThat(
            PROJECT_DIR.resolve(
                "build/" + NEW_BUILD_FOLDER + "/deploy/plan/flink-compiled-plan.json"))
        .content()
        .contains("stream-exec-window-table-function");
  }

  @Test
  void givenChangedUidFormat_whenCheckStateUpgrade_thenUnclaimedState() {
    var status =
        compileAndCheckUpgrade(
            new PackageSettings("window-count.sqrl", NO_STATE_TTL, DEFAULT_UID_FORMAT),
            new PackageSettings("window-count.sqrl", NO_STATE_TTL, "<transformation>_<id>"));

    assertThat(status.isSuccess()).as(status.getMessages()).isFalse();
    assertThat(status.getMessages())
        .contains(
            "table.exec.uid.format differs: <id>_<transformation> -> <transformation>_<id>, so"
                + " Flink gives each operator another uid [unclaimed state]");
  }

  @Test
  void givenWatermarkMovedToOtherColumnOfRank_whenCheckStateUpgrade_thenPasses()
      throws IOException {
    var status =
        compileAndCheckUpgrade("top-event-watermark-ts.sqrl", "top-event-watermark-ts2.sqrl");

    assertThat(status.isSuccess()).as(status.getMessages()).isTrue();
    assertThat(status.getMessages()).contains("The new Flink plan passes the state upgrade check");
    // The Rank stores both columns, and both columns change their form in the plan. The plan
    // writes a time attribute as an object with a "kind", and a regular timestamp as text.
    assertThat(rankColumnType(OLD_BUILD_FOLDER, "ts").path("kind").asText()).isEqualTo("ROWTIME");
    assertThat(rankColumnType(OLD_BUILD_FOLDER, "ts2").isTextual()).isTrue();
    assertThat(rankColumnType(NEW_BUILD_FOLDER, "ts").isTextual()).isTrue();
    assertThat(rankColumnType(NEW_BUILD_FOLDER, "ts2").path("kind").asText()).isEqualTo("ROWTIME");
  }

  /** The values that a test writes into the package template for one compile. */
  private record PackageSettings(String scriptFile, String stateTtl, String uidFormat) {}

  /** Compiles both scripts without a state TTL, see {@link #compileAndCheckUpgrade}. */
  private AssertStatusHook compileAndCheckUpgrade(String oldScriptFile, String newScriptFile) {
    return compileAndCheckUpgrade(oldScriptFile, NO_STATE_TTL, newScriptFile, NO_STATE_TTL);
  }

  /** Compiles both scripts with the default operator uid format. */
  private AssertStatusHook compileAndCheckUpgrade(
      String oldScriptFile, String oldStateTtl, String newScriptFile, String newStateTtl) {
    return compileAndCheckUpgrade(
        new PackageSettings(oldScriptFile, oldStateTtl, DEFAULT_UID_FORMAT),
        new PackageSettings(newScriptFile, newStateTtl, DEFAULT_UID_FORMAT));
  }

  /**
   * Compiles the old package settings into {@link #OLD_BUILD_FOLDER}. Then compiles the new package
   * settings into {@link #NEW_BUILD_FOLDER} with the check against the old build, and returns that
   * status.
   *
   * <p>Flink counts the operator ids in one counter for each JVM. A compile with the CLI runs in
   * its own JVM, so its ids start at 1. Both compiles of a test run in one JVM, so the test resets
   * the counter before each compile.
   */
  private AssertStatusHook compileAndCheckUpgrade(
      PackageSettings oldPackageSettings, PackageSettings newPackageSettings) {
    writePackage(oldPackageSettings);
    ExecNodeContext.resetIdCounter();
    var oldCompileStatus =
        compileExtension.execute(PROJECT_DIR, "compile", "-b", OLD_BUILD_FOLDER, tempPackageFile());
    assertThat(oldCompileStatus.isSuccess()).as(oldCompileStatus.getMessages()).isTrue();

    writePackage(newPackageSettings);
    ExecNodeContext.resetIdCounter();
    return compileExtension.execute(
        PROJECT_DIR,
        "compile",
        "-b",
        NEW_BUILD_FOLDER,
        "--check-state-upgrade-from",
        OLD_BUILD_FROM_PROJECT,
        tempPackageFile());
  }

  private void writePackage(PackageSettings packageSettings) {
    compileExtension.writeTempPackage(
        PROJECT_DIR.resolve(packageSettings.scriptFile()),
        Map.of(
            SCRIPT_FILE_TEMPLATE,
            packageSettings.scriptFile(),
            STATE_TTL_TEMPLATE,
            packageSettings.stateTtl(),
            UID_FORMAT_TEMPLATE,
            packageSettings.uidFormat()));
  }

  /** Returns the type of a column of the Rank operator in the compiled plan of a build folder. */
  private static JsonNode rankColumnType(String buildFolder, String columnName) throws IOException {
    var compiledPlan =
        new ObjectMapper()
            .readTree(
                PROJECT_DIR
                    .resolve("build/" + buildFolder + "/deploy/plan/flink-compiled-plan.json")
                    .toFile());
    for (var operator : compiledPlan.path("nodes")) {
      if (!operator.path("type").asText().startsWith("stream-exec-rank")) {
        continue;
      }
      for (var column : operator.path("outputType").path("fields")) {
        if (column.path("name").asText().equals(columnName)) {
          return column.path("fieldType");
        }
      }
    }
    throw new AssertionError(
        "The plan in " + buildFolder + " has no Rank operator with the column " + columnName);
  }

  private String tempPackageFile() {
    return compileExtension.getTempPackage().getFileName().toString();
  }
}
