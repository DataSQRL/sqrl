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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datasqrl.compile.CompilationProcess;
import com.datasqrl.compile.TestPlan;
import com.datasqrl.config.ExecutionEnginesHolder;
import com.datasqrl.config.PackageJson;
import com.datasqrl.config.TestRunnerConfiguration;
import com.datasqrl.engine.PhysicalPlan;
import com.datasqrl.error.CollectedException;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorMessage;
import com.datasqrl.packager.Packager;
import com.datasqrl.util.ConfigLoaderUtils;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

@ExtendWith(MockitoExtension.class)
class CompileCmdTest {

  private static final String SOURCE_OPERATOR =
      "{\"id\": 1, \"type\": \"stream-exec-table-source-scan_2\", \"description\": \"Orders\"}";

  private static final String SINK_OPERATOR =
      "{\"id\": 2, \"type\": \"stream-exec-sink_1\", \"description\": \"Sink(CustomerCount)\"}";

  private static final String OLD_COMPILED_PLAN =
      "{\"flinkVersion\": \"2.3\", \"nodes\": [" + SOURCE_OPERATOR + "," + SINK_OPERATOR + "]}";

  private static final String NEW_COMPILED_PLAN_WITH_ALL_OPERATORS = OLD_COMPILED_PLAN;

  private static final String NEW_COMPILED_PLAN_WITHOUT_SINK =
      "{\"flinkVersion\": \"2.3\", \"nodes\": [" + SOURCE_OPERATOR + "]}";

  @Mock private PackageJson sqrlConfig;
  @Mock private TestRunnerConfiguration testConfig;
  @Mock private ExecutionEnginesHolder engineHolder;
  @Mock private Packager packager;
  @Mock private CompilationProcess compilationProcess;

  @TempDir private Path tempDir;

  @Test
  void compile_whenProjectRootIsSet_resolvesTestDirFromProjectRoot() throws Exception {
    var projectRoot = tempDir.resolve("project");
    var packageJsonPath = projectRoot.resolve("package.json");
    var testDir = projectRoot.resolve("tests");
    Files.createDirectories(testDir);
    Files.writeString(packageJsonPath, "{}");

    var compileCmd = new CompileCmd();
    compileCmd.cli = new DatasqrlCli(tempDir, StatusHook.NONE, true);
    compileCmd.projectRootOption.projectRoot = Optional.of(Path.of("project"));

    var physicalPlan = PhysicalPlan.builder().build();
    var testPlan = new TestPlan();

    when(sqrlConfig.getTestConfig()).thenReturn(testConfig);
    when(testConfig.getTestDir(projectRoot)).thenReturn(Optional.of(testDir));
    when(compilationProcess.executeCompilation(Optional.of(testDir)))
        .thenReturn(Pair.of(physicalPlan, testPlan));

    try (var configLoader = mockStatic(ConfigLoaderUtils.class);
        var springContext =
            mockConstruction(
                AnnotationConfigApplicationContext.class,
                (mock, context) -> {
                  when(mock.getBean(ExecutionEnginesHolder.class)).thenReturn(engineHolder);
                  when(mock.getBean(Packager.class)).thenReturn(packager);
                  when(mock.getBean(CompilationProcess.class)).thenReturn(compilationProcess);
                })) {
      configLoader
          .when(
              () ->
                  ConfigLoaderUtils.loadUnresolvedConfig(
                      any(ErrorCollector.class), eq(List.of(packageJsonPath))))
          .thenReturn(sqrlConfig);

      compileCmd.compile(ErrorCollector.root());

      verify(testConfig).getTestDir(projectRoot);
      verify(testConfig, never()).getTestDir(tempDir);
      verify(compilationProcess).executeCompilation(Optional.of(testDir));
    }
  }

  @Test
  void givenInferredProjectRoot_whenCompiling_thenRegistersProjectRootForPreprocessing()
      throws Exception {
    var projectRoot = tempDir.resolve("project");
    var packageJsonPath = projectRoot.resolve("package.json");
    Files.createDirectory(projectRoot);
    Files.writeString(packageJsonPath, "{}");

    var compileCmd = new CompileCmd();
    compileCmd.cli = new DatasqrlCli(tempDir, StatusHook.NONE, true);
    compileCmd.packageFiles = List.of(Path.of("project", "package.json"));

    when(sqrlConfig.getTestConfig()).thenReturn(testConfig);
    when(testConfig.getTestDir(projectRoot)).thenReturn(Optional.empty());
    when(compilationProcess.executeCompilation(Optional.empty()))
        .thenReturn(Pair.of(PhysicalPlan.builder().build(), new TestPlan()));

    try (var configLoader = mockStatic(ConfigLoaderUtils.class);
        var springContext =
            mockConstruction(
                AnnotationConfigApplicationContext.class,
                (mock, context) -> {
                  when(mock.getBean(ExecutionEnginesHolder.class)).thenReturn(engineHolder);
                  when(mock.getBean(Packager.class)).thenReturn(packager);
                  when(mock.getBean(CompilationProcess.class)).thenReturn(compilationProcess);
                })) {
      configLoader
          .when(
              () ->
                  ConfigLoaderUtils.loadUnresolvedConfig(
                      any(ErrorCollector.class), eq(List.of(packageJsonPath))))
          .thenReturn(sqrlConfig);

      compileCmd.compile(ErrorCollector.root());

      verify(springContext.constructed().get(0))
          .registerBean(eq("projectRoot"), eq(Path.class), any(Supplier.class));
    }
  }

  @Test
  void givenInferredProjectRoot_whenFormattingPackageFiles_thenRemovesInferredRoot()
      throws IOException {
    var projectDir = tempDir.resolve(Path.of("apps", "orders"));
    Files.createDirectories(projectDir);

    var compileCmd = new CompileCmd();
    compileCmd.cli = new DatasqrlCli(tempDir, StatusHook.NONE, true);
    compileCmd.packageFiles = List.of(Path.of("apps", "orders", "package.json"));

    assertThat(compileCmd.formatGivenPackageFiles())
        .containsExactly(projectDir.resolve("package.json"));
  }

  @Test
  void givenExplicitProjectRoot_whenFormattingPackageFiles_thenResolvesFromExplicitRoot()
      throws IOException {
    var projRoot = Path.of("apps", "orders");
    var projectDir = tempDir.resolve(projRoot);
    Files.createDirectories(projectDir);

    var compileCmd = new CompileCmd();
    compileCmd.cli = new DatasqrlCli(tempDir, StatusHook.NONE, true);
    compileCmd.projectRootOption.projectRoot = Optional.of(projRoot);
    compileCmd.packageFiles = List.of(Path.of("package.json"));

    assertThat(compileCmd.formatGivenPackageFiles())
        .containsExactly(projectDir.resolve("package.json"));
  }

  @Test
  void givenNoBuildFolder_whenGettingBuildDir_thenUsesDefaultFolder() {
    var compileCmd = new CompileCmd();
    compileCmd.cli = new DatasqrlCli(tempDir, StatusHook.NONE, true);

    assertThat(compileCmd.getBuildDir()).isEqualTo(tempDir.resolve("build"));
  }

  @Test
  void
      givenCustomBuildFolderAndProjectRoot_whenGettingBuildAndDefaultTargetDirs_thenResolvesBothFromDefaultBuildFolder()
          throws IOException {
    var projectRoot = tempDir.resolve("project");
    Files.createDirectory(projectRoot);

    var compileCmd = new CompileCmd();
    compileCmd.cli = new DatasqrlCli(tempDir, StatusHook.NONE, true);
    compileCmd.projectRootOption.projectRoot = Optional.of(Path.of("project"));
    compileCmd.buildFolder = Optional.of(Path.of("output"));

    assertThat(compileCmd.getBuildDir()).isEqualTo(projectRoot.resolve("build/output"));
    assertThat(compileCmd.getTargetDir())
        .isEqualTo(projectRoot.resolve("build/output").resolve("deploy"));
  }

  @Test
  void givenAbsoluteBuildFolderOutsideTestExecution_whenGettingBuildDir_thenThrows() {
    var buildFolder = tempDir.resolve("output");
    var compileCmd = new CompileCmd();
    compileCmd.cli = new DatasqrlCli(tempDir, StatusHook.NONE, false);
    compileCmd.buildFolder = Optional.of(buildFolder);

    assertThatThrownBy(compileCmd::getBuildDir)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Build folder must be a relative path, got: " + buildFolder);
  }

  @Test
  void givenBuildFolderOutsideDefaultBuildFolder_whenGettingBuildDir_thenThrows()
      throws IOException {
    var projectRoot = tempDir.resolve("banking");
    Files.createDirectory(projectRoot);

    var compileCmd = new CompileCmd();
    compileCmd.cli = new DatasqrlCli(tempDir, StatusHook.NONE, false);
    compileCmd.projectRootOption.projectRoot = Optional.of(Path.of("banking"));
    compileCmd.buildFolder = Optional.of(Path.of("..", "build-banking"));

    assertThatThrownBy(compileCmd::getBuildDir)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Build folder '../build-banking' resolves to '"
                + projectRoot.resolve("build-banking")
                + "', which is outside build folder '"
                + projectRoot.resolve("build")
                + "'");
  }

  @Test
  void givenTargetFolderOutsideProjectRoot_whenGettingTargetDir_thenThrows() throws IOException {
    var projectRoot = tempDir.resolve("banking");
    Files.createDirectory(projectRoot);

    var compileCmd = new CompileCmd();
    compileCmd.cli = new DatasqrlCli(tempDir, StatusHook.NONE, false);
    compileCmd.projectRootOption.projectRoot = Optional.of(Path.of("banking"));
    compileCmd.targetFolder = Optional.of(Path.of("..", "deploy-banking"));

    assertThatThrownBy(compileCmd::getTargetDir)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Target folder '../deploy-banking' resolves to '"
                + tempDir.resolve("deploy-banking")
                + "', which is outside project root '"
                + projectRoot
                + "'");
  }

  @Test
  void
      givenOldBuildIsTheBuildFolderOfThisCompile_whenCompile_thenFailsBeforeCompileAndKeepsOldBuild()
          throws IOException {
    var oldPlanFile = createOldBuild("build/shop");
    var compileCmd = compileCmdWithOldBuild("build/shop", "package.json");

    assertThatThrownBy(() -> compileCmd.runInternal(ErrorCollector.root()))
        .isInstanceOf(CollectedException.class)
        .hasMessageContaining("is inside the build folder");
    assertThat(oldPlanFile).exists();
  }

  @Test
  void givenOldBuildIsTheBuildFolderAndOtherTargetFolder_whenCompile_thenFailsBeforeCompile()
      throws IOException {
    var oldPlanFile = createOldBuild("build/shop");
    var compileCmd = compileCmdWithOldBuild("build/shop", "package.json");
    compileCmd.targetFolder = Optional.of(Path.of("artifacts"));

    assertThatThrownBy(() -> compileCmd.runInternal(ErrorCollector.root()))
        .isInstanceOf(CollectedException.class)
        .hasMessageContaining("is inside the build folder");
    assertThat(oldPlanFile).exists();
  }

  @Test
  void givenNoBuildFolderOption_whenCompileWithOtherBuild_thenFailsBeforeCompileAndKeepsOldBuild()
      throws IOException {
    var oldPlanFile = createOldBuild("build/shop-replay");
    var compileCmd = compileCmdWithOldBuild("build/shop-replay", "package.json");
    compileCmd.buildFolder = Optional.empty();

    assertThatThrownBy(() -> compileCmd.runInternal(ErrorCollector.root()))
        .isInstanceOf(CollectedException.class)
        .hasMessageContaining("is inside the build folder");
    assertThat(oldPlanFile).exists();
  }

  @Test
  void givenTargetFolderOptionInsideOldBuild_whenCompile_thenFailsBeforeCompileAndKeepsOldBuild()
      throws IOException {
    var oldPlanFile = createOldBuild("build/shop-replay");
    var compileCmd = compileCmdWithOldBuild("build/shop-replay", "package.json");
    compileCmd.targetFolder = Optional.of(Path.of("build/shop-replay/deploy"));

    assertThatThrownBy(() -> compileCmd.runInternal(ErrorCollector.root()))
        .isInstanceOf(CollectedException.class)
        .hasMessageContaining("is inside the target folder");
    assertThat(oldPlanFile).exists();
  }

  @Test
  void givenCompatibleNewPlan_whenCompileWithStateUpgradeCheck_thenNoticeAndNoErrors()
      throws Exception {
    createOldBuild("build/shop-replay");
    var compileCmd = compileCmdWithOldBuild("build/shop-replay", "package.json");
    stubCompilationThatWrites(NEW_COMPILED_PLAN_WITH_ALL_OPERATORS);

    var errors = runCompile(compileCmd, ErrorCollector.root());

    assertThat(errors.hasErrors()).isFalse();
    assertThat(notices(errors)).anyMatch(notice -> notice.contains("can restore the state"));
  }

  @Test
  void givenThreePackageFiles_whenCompileWithStateUpgradeCheck_thenLoadsAllFilesAndChecks()
      throws Exception {
    createOldBuild("build/shop-replay");
    var compileCmd =
        compileCmdWithOldBuild(
            "build/shop-replay",
            "shop-shared-package.json",
            "shop-prod-package.json",
            "shop-extra-package.json");
    stubCompilationThatWrites(NEW_COMPILED_PLAN_WITH_ALL_OPERATORS);

    var errors = runCompile(compileCmd, ErrorCollector.root());

    assertThat(errors.hasErrors()).isFalse();
    assertThat(notices(errors)).anyMatch(notice -> notice.contains("can restore the state"));
  }

  @Test
  void givenNewPlanWithoutOldOperator_whenCompileWithStateUpgradeCheck_thenFailsWithBreak()
      throws Exception {
    createOldBuild("build/shop-replay");
    var compileCmd = compileCmdWithOldBuild("build/shop-replay", "package.json");
    stubCompilationThatWrites(NEW_COMPILED_PLAN_WITHOUT_SINK);

    assertThatThrownBy(() -> runCompile(compileCmd, ErrorCollector.root()))
        .isInstanceOf(CollectedException.class)
        .hasMessageContaining("operator 2 removed: Sink(CustomerCount)");
  }

  @Test
  void givenCompilationError_whenCompileWithStateUpgradeCheck_thenSkipsCheck() throws Exception {
    createOldBuild("build/shop-replay");
    var compileCmd = compileCmdWithOldBuild("build/shop-replay", "package.json");
    var errors = ErrorCollector.root();
    when(sqrlConfig.getTestConfig()).thenReturn(testConfig);
    when(testConfig.getTestDir(tempDir)).thenReturn(Optional.empty());
    when(compilationProcess.executeCompilation(Optional.empty()))
        .thenAnswer(
            invocation -> {
              errors.exception("Table Orders is not defined");
              return Pair.of(PhysicalPlan.builder().build(), new TestPlan());
            });

    runCompile(compileCmd, errors);

    assertThat(errors.hasErrors()).isTrue();
    assertThat(StreamSupport.stream(errors.spliterator(), false).map(ErrorMessage::getMessage))
        .containsExactly("Table Orders is not defined");
    verify(packager, never()).postprocess(any(), any(), any());
  }

  /** Writes {@link #OLD_COMPILED_PLAN} into the build folder and returns the plan file. */
  private Path createOldBuild(String oldBuildFolder) throws IOException {
    var oldPlanFile =
        tempDir.resolve(oldBuildFolder).resolve("deploy/plan/flink-compiled-plan.json");
    Files.createDirectories(oldPlanFile.getParent());
    Files.writeString(oldPlanFile, OLD_COMPILED_PLAN);
    return oldPlanFile;
  }

  /**
   * Returns {@code compile -b shop --check-state-upgrade-from <oldBuildFolder> <packageFiles>}, and
   * writes each package file into the project root.
   */
  private CompileCmd compileCmdWithOldBuild(String oldBuildFolder, String... packageFileNames)
      throws IOException {
    for (var packageFileName : packageFileNames) {
      Files.writeString(tempDir.resolve(packageFileName), "{}");
    }

    var compileCmd = new CompileCmd();
    compileCmd.cli = new DatasqrlCli(tempDir, StatusHook.NONE, true);
    compileCmd.packageFiles = Stream.of(packageFileNames).map(Path::of).toList();
    compileCmd.buildFolder = Optional.of(Path.of("shop"));
    compileCmd.checkStateUpgradeOption.oldBuildFolder = Optional.of(Path.of(oldBuildFolder));
    return compileCmd;
  }

  /** Makes the compile succeed, and makes the mocked packager write {@code newCompiledPlan}. */
  private void stubCompilationThatWrites(String newCompiledPlan) throws Exception {
    when(sqrlConfig.getTestConfig()).thenReturn(testConfig);
    when(testConfig.getTestDir(tempDir)).thenReturn(Optional.empty());
    when(compilationProcess.executeCompilation(Optional.empty()))
        .thenReturn(Pair.of(PhysicalPlan.builder().build(), new TestPlan()));
    doAnswer(
            invocation -> {
              Path newPlanDir = invocation.<Path>getArgument(0).resolve("plan");
              Files.createDirectories(newPlanDir);
              Files.writeString(newPlanDir.resolve("flink-compiled-plan.json"), newCompiledPlan);
              return null;
            })
        .when(packager)
        .postprocess(any(), any(), any());
  }

  /**
   * Runs the command (compile and check) with the mocked Spring context. The package configuration
   * loads only for the exact list of package files of the command, so a test fails if the compile
   * drops a file.
   */
  private ErrorCollector runCompile(CompileCmd compileCmd, ErrorCollector errors) throws Exception {
    var packageFilePaths = compileCmd.formatGivenPackageFiles();

    try (var configLoader = mockStatic(ConfigLoaderUtils.class);
        var springContext =
            mockConstruction(
                AnnotationConfigApplicationContext.class,
                (mock, context) -> {
                  when(mock.getBean(ExecutionEnginesHolder.class)).thenReturn(engineHolder);
                  when(mock.getBean(Packager.class)).thenReturn(packager);
                  when(mock.getBean(CompilationProcess.class)).thenReturn(compilationProcess);
                })) {
      configLoader
          .when(
              () ->
                  ConfigLoaderUtils.loadUnresolvedConfig(
                      any(ErrorCollector.class), eq(packageFilePaths)))
          .thenReturn(sqrlConfig);

      compileCmd.runInternal(errors);
      return errors;
    }
  }

  private static List<String> notices(ErrorCollector errors) {
    return StreamSupport.stream(errors.spliterator(), false)
        .filter(ErrorMessage::isNotice)
        .map(ErrorMessage::getMessage)
        .toList();
  }
}
