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
package com.datasqrl.cli;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datasqrl.cli.FlinkStateUpgradeCheck.FlinkJob;
import com.datasqrl.error.CollectedException;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorMessage;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.StreamSupport;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CheckStateUpgradeOptionTest {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private static final String SOURCE_OPERATOR =
      "{\"id\": 1, \"type\": \"stream-exec-table-source-scan_2\", \"description\": \"Orders\"}";

  private static final String SINK_OPERATOR =
      "{\"id\": 2, \"type\": \"stream-exec-sink_1\", \"description\": \"Sink(CustomerCount)\"}";

  @Nested
  class OldBuildFolder {

    @TempDir private Path projectRoot;

    @Test
    void givenOptionNotSet_whenCheckOldBuildFolderAndCheckUpgrade_thenNoMessages() {
      var option = new CheckStateUpgradeOption();
      var errors = ErrorCollector.root();

      option.checkOldBuildFolder(projectRoot, buildDir(), targetDir(), errors);
      option.checkUpgrade(targetDir(), errors);

      assertThat(errors.hasErrorsWarningsOrNotices()).isFalse();
    }

    @Test
    void givenOtherBuildFolder_whenCheckOldBuildFolder_thenNoErrors() throws IOException {
      writeCompiledPlan(projectRoot.resolve("build/shop-replay/deploy/plan"));

      assertThat(checkOldBuildFolder("build/shop-replay").hasErrors()).isFalse();
    }

    @Test
    void givenSiblingFolderWithSameNamePrefix_whenCheckOldBuildFolder_thenNoErrors()
        throws IOException {
      writeCompiledPlan(projectRoot.resolve("build/shop2/deploy/plan"));

      assertThat(checkOldBuildFolder("build/shop2").hasErrors()).isFalse();
    }

    @Test
    void givenBuildOfDefaultBuildFolder_whenCheckOldBuildFolderWithOtherBuildFolder_thenNoErrors()
        throws IOException {
      writeCompiledPlan(projectRoot.resolve("build/deploy/plan"));

      assertThat(checkOldBuildFolder("build").hasErrors()).isFalse();
    }

    @Test
    void givenMissingFolder_whenCheckOldBuildFolder_thenFatalDoesNotExist() {
      assertThatThrownBy(() -> checkOldBuildFolder("build/does-not-exist"))
          .isInstanceOf(CollectedException.class)
          .hasMessageContaining("does not exist");
    }

    @Test
    void givenFolderWithoutPlanFolder_whenCheckOldBuildFolder_thenFatalCannotFindPlanFolder()
        throws IOException {
      Files.createDirectories(projectRoot.resolve("sources"));

      assertThatThrownBy(() -> checkOldBuildFolder("sources"))
          .isInstanceOf(CollectedException.class)
          .hasMessageContaining("Cannot find the plan folder in");
    }

    @Test
    void givenPlanFile_whenCheckOldBuildFolder_thenFatalCannotFindPlanFolder() throws IOException {
      writeCompiledPlan(projectRoot.resolve("build/shop-replay/deploy/plan"));

      assertThatThrownBy(
              () -> checkOldBuildFolder("build/shop-replay/deploy/plan/flink-compiled-plan.json"))
          .isInstanceOf(CollectedException.class)
          .hasMessageContaining("Cannot find the plan folder in");
    }

    @Test
    void
        givenOldBuildIsTheBuildFolderOfThisCompile_whenCheckOldBuildFolder_thenFatalInsideBuildFolder()
            throws IOException {
      writeCompiledPlan(targetDir().resolve("plan"));

      assertThatThrownBy(() -> checkOldBuildFolder("build/shop"))
          .isInstanceOf(CollectedException.class)
          .hasMessageContaining("is inside the build folder")
          .hasMessageContaining(buildDir().toString());
    }

    @Test
    void givenOldBuildInsideDefaultBuildFolder_whenCheckOldBuildFolder_thenFatalInsideBuildFolder()
        throws IOException {
      writeCompiledPlan(projectRoot.resolve("build/shop-replay/deploy/plan"));
      var defaultBuildDir = projectRoot.resolve("build");

      assertThatThrownBy(
              () ->
                  option("build/shop-replay")
                      .checkOldBuildFolder(
                          projectRoot,
                          defaultBuildDir,
                          defaultBuildDir.resolve("deploy"),
                          ErrorCollector.root()))
          .isInstanceOf(CollectedException.class)
          .hasMessageContaining("is inside the build folder");
    }

    @Test
    void
        givenSymbolicLinkToBuildFolderOfThisCompile_whenCheckOldBuildFolder_thenFatalInsideBuildFolder()
            throws IOException {
      writeCompiledPlan(targetDir().resolve("plan"));
      Files.createSymbolicLink(projectRoot.resolve("link-to-shop"), buildDir());

      assertThatThrownBy(() -> checkOldBuildFolder("link-to-shop"))
          .isInstanceOf(CollectedException.class)
          .hasMessageContaining("is inside the build folder");
    }

    @Test
    void
        givenSymbolicLinkInsideBuildFolderOfThisCompile_whenCheckOldBuildFolder_thenFatalInsideBuildFolder()
            throws IOException {
      var deployedBuild = projectRoot.resolve("deployed");
      writeCompiledPlan(deployedBuild.resolve("deploy/plan"));
      Files.createDirectories(buildDir());
      Files.createSymbolicLink(buildDir().resolve("deployed"), deployedBuild);

      assertThatThrownBy(() -> checkOldBuildFolder("build/shop/deployed"))
          .isInstanceOf(CollectedException.class)
          .hasMessageContaining("is inside the build folder")
          .hasMessageContaining("the symbolic link to it");
    }

    @Test
    void givenOldBuildInsideTargetFolderOption_whenCheckOldBuildFolder_thenFatalInsideTargetFolder()
        throws IOException {
      writeCompiledPlan(projectRoot.resolve("artifacts/plan"));
      var targetFolderOption = projectRoot.resolve("artifacts");

      assertThatThrownBy(
              () ->
                  option("artifacts")
                      .checkOldBuildFolder(
                          projectRoot, buildDir(), targetFolderOption, ErrorCollector.root()))
          .isInstanceOf(CollectedException.class)
          .hasMessageContaining("is inside the target folder");
    }

    @Test
    void givenRelativeBuildFolder_whenCheckUpgrade_thenComparesDeployPlanFromProjectRoot()
        throws IOException {
      var oldPlanDir = writeCompiledPlan(projectRoot.resolve("build/shop-replay/deploy/plan"));

      assertThat(checkUpgradeNotices("build/shop-replay"))
          .singleElement()
          .asString()
          .contains("passes the state upgrade check against the old build in " + oldPlanDir);
    }

    @Test
    void givenTargetFolder_whenCheckUpgrade_thenComparesItsPlanFolder() throws IOException {
      var oldPlanDir = writeCompiledPlan(projectRoot.resolve("artifacts/plan"));

      assertThat(checkUpgradeNotices("artifacts"))
          .singleElement()
          .asString()
          .contains("old build in " + oldPlanDir);
    }

    @Test
    void givenPlanFolder_whenCheckUpgrade_thenComparesIt() throws IOException {
      var oldPlanDir = writeCompiledPlan(projectRoot.resolve("deployed-plan"));

      assertThat(checkUpgradeNotices("deployed-plan"))
          .singleElement()
          .asString()
          .contains("old build in " + oldPlanDir);
    }

    @Test
    void givenTargetFolderOptionInsideOldTargetFolder_whenCheckUpgrade_thenComparesOldPlanFolder()
        throws IOException {
      assertCheckUpgradeComparesOldPlanFolder(
          "artifacts",
          projectRoot.resolve("artifacts/plan"),
          projectRoot.resolve("artifacts/deploy"));
    }

    @Test
    void givenTargetFolderOptionInsideOldPlanFolder_whenCheckUpgrade_thenComparesOldPlanFolder()
        throws IOException {
      assertCheckUpgradeComparesOldPlanFolder(
          "deployed-plan",
          projectRoot.resolve("deployed-plan"),
          projectRoot.resolve("deployed-plan/deploy"));
    }

    @Test
    void givenOptionSetWithoutCheckOldBuildFolder_whenCheckUpgrade_thenIllegalState() {
      var option = option("build/shop-replay");

      assertThatThrownBy(() -> option.checkUpgrade(targetDir(), ErrorCollector.root()))
          .isInstanceOf(IllegalStateException.class)
          .hasMessage("Call checkOldBuildFolder before checkUpgrade");
    }

    @Test
    void givenBuildFolderWithoutFlinkPlan_whenCheckUpgrade_thenNoticeNoFlinkPlan()
        throws IOException {
      Files.createDirectories(projectRoot.resolve("build/shop-replay/deploy/plan"));

      assertThat(checkUpgradeNotices("build/shop-replay"))
          .singleElement()
          .asString()
          .contains("has no Flink plan");
    }

    private CheckStateUpgradeOption option(String oldBuildFolder) {
      var option = new CheckStateUpgradeOption();
      option.oldBuildFolder = Optional.of(Path.of(oldBuildFolder));
      return option;
    }

    /** Checks the folder for a compile with {@code -b shop}. */
    private ErrorCollector checkOldBuildFolder(String oldBuildFolder) {
      var errors = ErrorCollector.root();
      option(oldBuildFolder).checkOldBuildFolder(projectRoot, buildDir(), targetDir(), errors);
      return errors;
    }

    /**
     * Checks the folder, writes a compatible new plan into the target folder as the compile does,
     * and returns the notices of the check.
     */
    private List<String> checkUpgradeNotices(String oldBuildFolder) throws IOException {
      var option = option(oldBuildFolder);
      var errors = ErrorCollector.root();
      option.checkOldBuildFolder(projectRoot, buildDir(), targetDir(), errors);
      writeCompiledPlan(targetDir().resolve("plan"));

      option.checkUpgrade(targetDir(), errors);

      assertThat(errors.hasErrors()).isFalse();
      return messages(errors, ErrorMessage::isNotice);
    }

    /**
     * Runs the check for a compile whose {@code -t} folder is inside the old build. The old plan
     * has a sink that the new plan removes, so the check fails only when it reads the old plan.
     */
    private void assertCheckUpgradeComparesOldPlanFolder(
        String oldBuildFolder, Path oldPlanDir, Path targetFolderOption) throws IOException {
      Files.createDirectories(oldPlanDir);
      Files.writeString(
          oldPlanDir.resolve("flink-compiled-plan.json"),
          plan("2.3", SOURCE_OPERATOR, SINK_OPERATOR));
      var option = option(oldBuildFolder);
      var errors = ErrorCollector.root();

      option.checkOldBuildFolder(projectRoot, buildDir(), targetFolderOption, errors);
      // The compile writes the new plan to <target folder>/plan. A new search in the old build
      // finds this folder first, because it is <old build>/deploy/plan.
      writeCompiledPlan(targetFolderOption.resolve("plan"));

      assertThatThrownBy(() -> option.checkUpgrade(targetFolderOption, errors))
          .isInstanceOf(CollectedException.class)
          .hasMessageContaining("cannot restore the state of the old build in " + oldPlanDir)
          .hasMessageContaining("operator id=2 removed: Sink(CustomerCount)");
    }

    /** The build folder of a compile with {@code -b shop}. */
    private Path buildDir() {
      return projectRoot.resolve("build/shop");
    }

    private Path targetDir() {
      return buildDir().resolve("deploy");
    }

    private static Path writeCompiledPlan(Path planDir) throws IOException {
      Files.createDirectories(planDir);
      Files.writeString(planDir.resolve("flink-compiled-plan.json"), plan("2.3", SOURCE_OPERATOR));
      return planDir;
    }
  }

  @Nested
  class CompareFlinkJobs {

    private static final Path OLD_PLAN_DIR = Path.of("build/deploy/plan");

    @Test
    void givenCompatiblePlans_whenCompareFlinkJobs_thenNoticeAndNoErrors() {
      var errors = ErrorCollector.root();

      CheckStateUpgradeOption.compareFlinkJobs(
          OLD_PLAN_DIR,
          flinkJob(plan("2.3", SOURCE_OPERATOR), Optional.of(128)),
          flinkJob(plan("2.3", SOURCE_OPERATOR, SINK_OPERATOR), Optional.of(128)),
          errors);

      assertThat(errors.hasErrors()).isFalse();
      assertThat(messages(errors, ErrorMessage::isNotice))
          .singleElement()
          .asString()
          .contains("passes the state upgrade check");
    }

    @Test
    void givenRemovedOperatorWithState_whenCompareFlinkJobs_thenMarkedLineAndUnclaimedStateNote() {
      var oldFlinkJob =
          flinkJob(plan("2.3", SOURCE_OPERATOR, deduplicateOperator("0 ms")), Optional.empty());
      var newFlinkJob = flinkJob(plan("2.3", SOURCE_OPERATOR), Optional.empty());

      assertThatThrownBy(
              () ->
                  CheckStateUpgradeOption.compareFlinkJobs(
                      OLD_PLAN_DIR, oldFlinkJob, newFlinkJob, ErrorCollector.root()))
          .isInstanceOf(CollectedException.class)
          .hasMessageContaining("cannot restore the state")
          .hasMessageContaining(
              "operator id=5 removed: Deduplicate(keep=[LastRow], key=[id]) [unclaimed state]")
          .hasMessageContaining(
              "The lines marked [unclaimed state] leave old state that no operator of the new plan"
                  + " claims.")
          .hasMessageContaining("execution.state-recovery.ignore-unclaimed-state is true")
          .hasMessageNotContaining("[incompatible state]");
    }

    @Test
    void givenRemovedOperatorWithoutState_whenCompareFlinkJobs_thenNoMarkAndNoNote() {
      var calcOperator =
          "{\"id\": 2, \"type\": \"stream-exec-calc_1\", \"description\": \"Calc(select=[id])\"}";
      var oldFlinkJob = flinkJob(plan("2.3", SOURCE_OPERATOR, calcOperator), Optional.empty());
      var newFlinkJob = flinkJob(plan("2.3", SOURCE_OPERATOR), Optional.empty());

      assertThatThrownBy(
              () ->
                  CheckStateUpgradeOption.compareFlinkJobs(
                      OLD_PLAN_DIR, oldFlinkJob, newFlinkJob, ErrorCollector.root()))
          .isInstanceOf(CollectedException.class)
          .hasMessageContaining("operator id=2 removed: Calc(select=[id])")
          .hasMessageNotContaining("[unclaimed state]")
          .hasMessageNotContaining("[incompatible state]")
          .hasMessageNotContaining("ignore-unclaimed-state");
    }

    @Test
    void givenUnclaimedAndIncompatibleState_whenCompareFlinkJobs_thenBothMarksAndBothNotes() {
      var oldDeduplicateOperator =
          "{\"id\": 4, \"type\": \"stream-exec-deduplicate_1\", \"description\": \"Deduplicate\","
              + " \"state\": [{\"index\": 0, \"name\": \"deduplicateState\"}], \"outputType\":"
              + " \"ROW<`id` INT>\"}";
      var newDeduplicateOperator =
          oldDeduplicateOperator.replace("ROW<`id` INT>", "ROW<`id` INT, `extra` INT>");
      var oldFlinkJob =
          flinkJob(
              plan("2.3", oldDeduplicateOperator, deduplicateOperator("0 ms")), Optional.empty());
      var newFlinkJob = flinkJob(plan("2.3", newDeduplicateOperator), Optional.empty());

      assertThatThrownBy(
              () ->
                  CheckStateUpgradeOption.compareFlinkJobs(
                      OLD_PLAN_DIR, oldFlinkJob, newFlinkJob, ErrorCollector.root()))
          .isInstanceOf(CollectedException.class)
          .hasMessageContaining(
              "operator id=4 changed: field 'outputType' differs: ROW<`id` INT> -> ROW<`id` INT,"
                  + " `extra` INT> (Deduplicate) [incompatible state]")
          .hasMessageContaining(
              "operator id=5 removed: Deduplicate(keep=[LastRow], key=[id]) [unclaimed state]")
          .hasMessageContaining("The lines marked [unclaimed state] leave old state")
          .hasMessageContaining(
              "The lines marked [incompatible state] belong to an operator that claims the old state"
                  + " but cannot read it.");
    }

    @Test
    void givenDescriptionWithPercentSign_whenCompareFlinkJobs_thenMessageKeepsIt() {
      var operatorWithPercentSign =
          "{\"id\": 2, \"type\": \"stream-exec-calc_1\", \"description\": \"Calc(where=[LIKE(name, '%a%')])\"}";
      var oldFlinkJob = flinkJob(plan("2.3", operatorWithPercentSign), Optional.empty());
      var newFlinkJob = flinkJob(plan("2.3"), Optional.empty());

      assertThatThrownBy(
              () ->
                  CheckStateUpgradeOption.compareFlinkJobs(
                      OLD_PLAN_DIR, oldFlinkJob, newFlinkJob, ErrorCollector.root()))
          .isInstanceOf(CollectedException.class)
          .hasMessageContaining("LIKE(name, '%a%')");
    }

    @Test
    void givenOldBuildWithoutFlinkPlan_whenCompareFlinkJobs_thenNoticeAndNoErrors() {
      var errors = ErrorCollector.root();

      CheckStateUpgradeOption.compareFlinkJobs(
          OLD_PLAN_DIR,
          flinkJob(null, Optional.empty()),
          flinkJob(plan("2.3", SOURCE_OPERATOR), Optional.empty()),
          errors);

      assertThat(errors.hasErrors()).isFalse();
      assertThat(messages(errors, ErrorMessage::isNotice))
          .singleElement()
          .asString()
          .contains("old build", "has no Flink plan");
    }

    @Test
    void givenOldBuildWithoutFlinkPlanAndNewMaxParallelism_whenCompareFlinkJobs_thenNoWarning() {
      var errors = ErrorCollector.root();

      CheckStateUpgradeOption.compareFlinkJobs(
          OLD_PLAN_DIR,
          flinkJob(null, Optional.empty()),
          flinkJob(plan("2.3", SOURCE_OPERATOR), Optional.of(144)),
          errors);

      assertThat(messages(errors, ErrorMessage::isWarning)).isEmpty();
      assertThat(messages(errors, ErrorMessage::isNotice))
          .singleElement()
          .asString()
          .contains("has no Flink plan");
    }

    @Test
    void givenNewBuildWithoutFlinkPlan_whenCompareFlinkJobs_thenNoticeAndNoErrors() {
      var errors = ErrorCollector.root();

      CheckStateUpgradeOption.compareFlinkJobs(
          OLD_PLAN_DIR,
          flinkJob(plan("2.3", SOURCE_OPERATOR), Optional.empty()),
          flinkJob(null, Optional.empty()),
          errors);

      assertThat(errors.hasErrors()).isFalse();
      assertThat(messages(errors, ErrorMessage::isNotice))
          .singleElement()
          .asString()
          .contains("new compile has no Flink plan");
    }

    @Test
    void givenOnlyStateTtlChanged_whenCompareFlinkJobs_thenNoticeAndNoWarning() {
      var errors = ErrorCollector.root();

      CheckStateUpgradeOption.compareFlinkJobs(
          OLD_PLAN_DIR,
          flinkJob(plan("2.3", deduplicateOperator("0 ms")), Optional.empty()),
          flinkJob(plan("2.3", deduplicateOperator("1 h")), Optional.empty()),
          errors);

      assertThat(errors.hasErrors()).isFalse();
      assertThat(messages(errors, ErrorMessage::isWarning)).isEmpty();
      assertThat(messages(errors, ErrorMessage::isNotice))
          .anySatisfy(
              message ->
                  assertThat(message)
                      .contains("the TTL of state 'deduplicateState' changes: 0 ms -> 1 h"));
    }

    private static String deduplicateOperator(String ttl) {
      return ("{\"id\": 5, \"type\": \"stream-exec-deduplicate_1\", \"description\":"
              + " \"Deduplicate(keep=[LastRow], key=[id])\", \"state\": [{\"index\": 0, \"ttl\":"
              + " \"%s\", \"name\": \"deduplicateState\"}]}")
          .formatted(ttl);
    }

    @Test
    void givenDifferentFlinkVersion_whenCompareFlinkJobs_thenFlinkVersionNotice() {
      var errors = ErrorCollector.root();

      CheckStateUpgradeOption.compareFlinkJobs(
          OLD_PLAN_DIR,
          flinkJob(plan("2.2", SOURCE_OPERATOR), Optional.empty()),
          flinkJob(plan("2.3", SOURCE_OPERATOR), Optional.empty()),
          errors);

      assertThat(errors.hasErrors()).isFalse();
      assertThat(messages(errors, ErrorMessage::isNotice))
          .anySatisfy(message -> assertThat(message).contains("2.2 -> 2.3"));
    }

    @Test
    void givenDifferentMaxParallelism_whenCompareFlinkJobs_thenFatal() {
      var oldFlinkJob = flinkJob(plan("2.3", SOURCE_OPERATOR), Optional.of(128));
      var newFlinkJob = flinkJob(plan("2.3", SOURCE_OPERATOR), Optional.of(144));

      assertThatThrownBy(
              () ->
                  CheckStateUpgradeOption.compareFlinkJobs(
                      OLD_PLAN_DIR, oldFlinkJob, newFlinkJob, ErrorCollector.root()))
          .isInstanceOf(CollectedException.class)
          .hasMessageContaining("pipeline.max-parallelism differs: 128 -> 144");
    }

    @Test
    void givenOnlyOldMaxParallelism_whenCompareFlinkJobs_thenWarning() {
      var errors = ErrorCollector.root();

      CheckStateUpgradeOption.compareFlinkJobs(
          OLD_PLAN_DIR,
          flinkJob(plan("2.3", SOURCE_OPERATOR), Optional.of(128)),
          flinkJob(plan("2.3", SOURCE_OPERATOR), Optional.empty()),
          errors);

      assertThat(errors.hasErrors()).isFalse();
      assertThat(messages(errors, ErrorMessage::isWarning))
          .singleElement()
          .asString()
          .contains("Only the old build sets pipeline.max-parallelism (128)")
          .contains("Flink takes this value from the savepoint.");
    }

    @Test
    void givenOnlyNewMaxParallelism_whenCompareFlinkJobs_thenWarning() {
      var errors = ErrorCollector.root();

      CheckStateUpgradeOption.compareFlinkJobs(
          OLD_PLAN_DIR,
          flinkJob(plan("2.3", SOURCE_OPERATOR), Optional.empty()),
          flinkJob(plan("2.3", SOURCE_OPERATOR), Optional.of(144)),
          errors);

      assertThat(errors.hasErrors()).isFalse();
      assertThat(messages(errors, ErrorMessage::isWarning))
          .singleElement()
          .asString()
          .contains("Only the new build sets pipeline.max-parallelism (144)");
    }

    /**
     * The Flink job of a build, or a build without a Flink job when {@code compiledPlan} is null.
     */
    private static Optional<FlinkJob> flinkJob(
        String compiledPlan, Optional<Integer> maxParallelism) {
      return Optional.ofNullable(compiledPlan)
          .map(plan -> new FlinkJob(json(plan), maxParallelism));
    }
  }

  @Nested
  class ReadFlinkJob {

    private static final String EMPTY_COMPILED_PLAN = "{\"flinkVersion\": \"2.3\", \"nodes\": []}";

    @TempDir private Path planDir;

    @Test
    void givenCompiledPlanAndMaxParallelism_whenReadFlinkJob_thenReadsBoth() throws IOException {
      writeCompiledPlan(EMPTY_COMPILED_PLAN);
      writeFlinkConfig("pipeline.max-parallelism: 144\n");

      assertThat(CheckStateUpgradeOption.readFlinkJob(planDir, ErrorCollector.root()))
          .hasValueSatisfying(
              flinkJob -> {
                assertThat(flinkJob.compiledPlan().path("flinkVersion").asText()).isEqualTo("2.3");
                assertThat(flinkJob.maxParallelism()).contains(144);
              });
    }

    @Test
    void givenFlinkConfigWithoutMaxParallelism_whenReadFlinkJob_thenMaxParallelismEmpty()
        throws IOException {
      writeCompiledPlan(EMPTY_COMPILED_PLAN);
      writeFlinkConfig("parallelism.default: 2\n");

      assertThat(CheckStateUpgradeOption.readFlinkJob(planDir, ErrorCollector.root()))
          .hasValueSatisfying(flinkJob -> assertThat(flinkJob.maxParallelism()).isEmpty());
    }

    @Test
    void givenPlanDirWithoutFlinkFiles_whenReadFlinkJob_thenNoFlinkJob() throws IOException {
      Files.writeString(planDir.resolve("postgres.json"), "{}");

      assertThat(CheckStateUpgradeOption.readFlinkJob(planDir, ErrorCollector.root())).isEmpty();
    }

    @Test
    void givenFlinkConfigWithoutCompiledPlan_whenReadFlinkJob_thenNoFlinkJob() throws IOException {
      writeFlinkConfig("pipeline.max-parallelism: 144\n");

      assertThat(CheckStateUpgradeOption.readFlinkJob(planDir, ErrorCollector.root())).isEmpty();
    }

    @Test
    void givenInvalidYaml_whenReadFlinkJob_thenFatalWithFilePath() throws IOException {
      writeCompiledPlan(EMPTY_COMPILED_PLAN);
      writeFlinkConfig("pipeline.max-parallelism: [unclosed\n");

      assertThatThrownBy(() -> CheckStateUpgradeOption.readFlinkJob(planDir, ErrorCollector.root()))
          .isInstanceOf(CollectedException.class)
          .hasMessageContaining("Cannot read " + planDir.resolve("flink-config.yaml"));
    }

    @Test
    void givenInvalidMaxParallelism_whenReadFlinkJob_thenFatalWithFilePath() throws IOException {
      writeCompiledPlan(EMPTY_COMPILED_PLAN);
      writeFlinkConfig("pipeline.max-parallelism: abc\n");

      assertThatThrownBy(() -> CheckStateUpgradeOption.readFlinkJob(planDir, ErrorCollector.root()))
          .isInstanceOf(CollectedException.class)
          .hasMessageContaining("Cannot read " + planDir.resolve("flink-config.yaml"))
          .hasMessageContaining("abc");
    }

    @Test
    void givenFolderWithCompiledPlan_whenHasFlinkFiles_thenTrue() throws IOException {
      writeCompiledPlan("{}");

      assertThat(CheckStateUpgradeOption.hasFlinkFiles(planDir)).isTrue();
    }

    @Test
    void givenFolderWithoutFlinkFiles_whenHasFlinkFiles_thenFalse() {
      assertThat(CheckStateUpgradeOption.hasFlinkFiles(planDir)).isFalse();
    }

    private void writeCompiledPlan(String content) throws IOException {
      Files.writeString(planDir.resolve("flink-compiled-plan.json"), content);
    }

    private void writeFlinkConfig(String content) throws IOException {
      Files.writeString(planDir.resolve("flink-config.yaml"), content);
    }
  }

  private static List<String> messages(
      ErrorCollector errors, Predicate<ErrorMessage> severityFilter) {
    return StreamSupport.stream(errors.spliterator(), false)
        .filter(severityFilter)
        .map(ErrorMessage::getMessage)
        .toList();
  }

  private static String plan(String flinkVersion, String... operators) {
    return "{\"flinkVersion\": \"%s\", \"nodes\": [%s]}"
        .formatted(flinkVersion, String.join(",", operators));
  }

  @SneakyThrows
  private static JsonNode json(String text) {
    return OBJECT_MAPPER.readTree(text);
  }
}
