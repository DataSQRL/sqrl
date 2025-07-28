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

import com.datasqrl.config.PackageJson;
import com.datasqrl.config.SqrlConstants;
import com.datasqrl.engines.TestContainersForTestGoal;
import com.datasqrl.engines.TestContainersForTestGoal.TestContainerHook;
import com.datasqrl.engines.TestEngine.EngineFactory;
import com.datasqrl.engines.TestEngines;
import com.datasqrl.engines.TestExecutionEnv;
import com.datasqrl.engines.TestExecutionEnv.TestEnvContext;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.tests.TestExtension;
import com.datasqrl.tests.UseCaseTestExtensions;
import com.datasqrl.util.ConfigLoaderUtils;
import com.datasqrl.util.SnapshotTest.Snapshot;
import com.datasqrl.util.TestScriptCompileExtension;
import com.datasqrl.util.TestShardingExtension;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Runs and Tests the uses cases in `resources/usecases`. This stands up the entire compiled
 * pipeline and runs or tests the pipeline. Add use cases to the folder above to validate full
 * pipeline execution.
 *
 * <p>Note, that this test is resource intensive and slow.
 */
@Slf4j
@ExtendWith({
  MiniClusterExtension.class,
  TestShardingExtension.class,
  TestScriptCompileExtension.class
})
public class FullUseCasesIT {

  private static final Path USE_CASES = Path.of("src/test/resources/usecases");

  private static final Set<String> DISABLED_USE_CASES =
      Set.of(
          "package-flink-only.json", // not a full test case
          "package-flink-functions.json", // not a full test case
          "package-conference.json", // fails in build server
          "package-iceberg-export.json", // fails in build server
          "package-duckdb.json", // fails in build server
          "package-snowflake.json", // fails in build server
          "package-sensors-full.json", // flaky (too much data)
          "package-analytics-only.json",
          "package-postgres-log.json",
          "package-connectors.json" // should not be executed
          );

  private static TestContainerHook containerHook;

  UseCaseTestExtensions testExtensions = new UseCaseTestExtensions();

  @BeforeAll
  static void beforeAll() {
    var engines = new EngineFactory().createAll();

    containerHook = engines.accept(new TestContainersForTestGoal(), null);
    containerHook.start();
  }

  @AfterAll
  static void afterAll() {
    if (containerHook != null) {
      containerHook.teardown();
    }
  }

  @AfterEach
  void afterEach() {
    if (containerHook != null) {
      containerHook.clear();
    }
  }

  @Disabled("Intended for manual usage")
  @ParameterizedTest
  @MethodSource("specificUseCaseProvider")
  void specificUseCase(UseCaseParam param) {
    useCase(param);
  }

  //////////////////////////////////////////////////////////////////
  ////////////// MODIFY THIS TO RUN SPECIFIC USE CASE //////////////
  //////////////////////////////////////////////////////////////////
  private static Stream<UseCaseParam> specificUseCaseProvider() {
    var useCaseFolderName = "clickstream";

    var packageFileName = getPackageFileName(useCaseFolderName);
    var pkg = USE_CASES.resolve(useCaseFolderName).resolve(packageFileName);
    assertThat(pkg).isRegularFile();

    return Stream.of(new UseCaseParam(pkg, "test", -1));
  }

  @ParameterizedTest
  @MethodSource("nonDisabledUseCaseProvider")
  void useCase(UseCaseParam param) {
    log.info("Testing {}", param.getPackageJsonName());

    var snapshot =
        Snapshot.of(
            FullUseCasesIT.class,
            param.getUseCaseName(),
            param.getPackageJsonName().substring(0, param.getPackageJsonName().length() - 5));

    TestExtension testExtension = testExtensions.create(param.getUseCaseName());
    try {
      testExtension.setup();

      // TODO: move this to test env setup
      Path rootDir = param.packageJsonPath().getParent();
      PackageJson packageJson =
          ConfigLoaderUtils.loadResolvedConfig(
              ErrorCollector.root(), rootDir.resolve(SqrlConstants.BUILD_DIR_NAME));

      TestEngines engines = new EngineFactory().create(packageJson);

      log.info(
          """
        The test parameters
        Test name: {}
        Test path: {}
        Test package file: {}
        """,
          param.getUseCaseName(),
          rootDir,
          param.getPackageJsonName());

      // Execute the test
      TestEnvContext context = new TestEnvContext(rootDir, containerHook.getEnv(), param);

      engines.accept(
          new TestExecutionEnv(
              param.getPackageJsonName() + ":" + param.goal(), packageJson, rootDir, snapshot),
          context);

    } finally {
      testExtension.teardown();
      containerHook.clear();
    }

    if (snapshot.hasContent()) {
      snapshot.createOrValidate();
    }
  }

  @SneakyThrows
  private static Set<UseCaseParam> nonDisabledUseCaseProvider() {
    var useCasesDir = USE_CASES.toAbsolutePath();

    try (var useCaseStream = Files.list(useCasesDir)) {
      // First collect and sort all paths to ensure deterministic ordering
      var sortedPaths =
          useCaseStream
              .filter(Files::isDirectory)
              .flatMap(FullUseCasesIT::collectPackageJsonFiles)
              .sorted() // Ensure consistent ordering for index assignment
              .toList();

      // Assign sequential indices and create UseCaseParam objects
      return IntStream.range(0, sortedPaths.size())
          .mapToObj(i -> new UseCaseParam(sortedPaths.get(i), "test", i))
          .collect(Collectors.toCollection(TreeSet::new));
    }
  }

  /**
   * Collect files that match the {@code package-<use-case-name>.json} pattern from a given use case
   * dir that are not listed in {@code DISABLED_USE_CASES}.
   */
  @SneakyThrows
  private static Stream<Path> collectPackageJsonFiles(Path useCaseDir) {
    var useCaseName = useCaseDir.getFileName().toString();

    try (var stream = Files.list(useCaseDir)) {
      return stream
          .filter(Files::isRegularFile)
          .filter(p -> p.getFileName().toString().equals(getPackageFileName(useCaseName)))
          .filter(p -> !DISABLED_USE_CASES.contains(p.getFileName().toString()))
          .toList()
          .stream();
    }
  }

  private static String getPackageFileName(String useCaseName) {
    return String.format("package-%s.json", useCaseName);
  }
}
