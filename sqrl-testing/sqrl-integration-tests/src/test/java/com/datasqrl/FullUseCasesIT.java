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

import static org.assertj.core.api.Assertions.fail;
import static org.assertj.core.api.Assumptions.assumeThat;

import com.datasqrl.cli.AssertStatusHook;
import com.datasqrl.cli.DatasqrlRun;
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
import com.datasqrl.util.FlinkOperatorStatusChecker;
import com.datasqrl.util.SnapshotTest.Snapshot;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
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
@ExtendWith(MiniClusterExtension.class)
class FullUseCasesIT {

  private static final Path USE_CASES = Path.of("src/test/resources/usecases");

  private static Integer totalShards;
  private static int shardIdx;
  private static int shard;

  record ScriptCriteria(String name, String goal) {}

  List<ScriptCriteria> disabledScripts =
      List.of(
          new ScriptCriteria("flink-only.sqrl", "test"), // not a full test case
          new ScriptCriteria("flink-functions.sqrl", "test"), // not a full test case
          new ScriptCriteria("flink-functions.sqrl", "run"), // not a full test case
          new ScriptCriteria("conference-disabled.sqrl", "test"), // fails in build server
          new ScriptCriteria("conference-disabled.sqrl", "run"), // fails in build server
          new ScriptCriteria("iceberg-export.sqrl", "test"), // fails in build server
          new ScriptCriteria("iceberg-export.sqrl", "run"), // fails in build server
          new ScriptCriteria("duckdb.sqrl", "test"), // fails in build server
          new ScriptCriteria("duckdb.sqrl", "run"), // fails in build server
          new ScriptCriteria("snowflake-disabled.sqrl", "test"), // fails in build server
          new ScriptCriteria("snowflake-disabled.sqrl", "run"), // fails in build server
          new ScriptCriteria("sensors-mutation.sqrl", "run"), // flaky see sqrl script
          new ScriptCriteria("sensors-full.sqrl", "test"), // flaky (too much data)
          new ScriptCriteria("sensors-full.sqrl", "run"), // flaky (too much data)
          new ScriptCriteria("analytics-only.sqrl", "test"),
          new ScriptCriteria("analytics-only.sqrl", "run"),
          new ScriptCriteria("postgres-log-disabled.sqrl", "test"),
          new ScriptCriteria("postgres-log-disabled.sqrl", "run"),
          new ScriptCriteria("connectors.sqrl", "test"), // should not be executed
          new ScriptCriteria("flink_kafka.sqrl", "run"), // does not expose an API
          new ScriptCriteria("minimalFlink.sqrl", "run"), // only want to test this
          // only want to test this, no way to authenticate on runs
          new ScriptCriteria("jwt-authorized.sqrl", "run"),
          new ScriptCriteria("jwt-unauthorized.sqrl", "run"),
          new ScriptCriteria(
              "temporal-join.sqrl",
              "run") // TODO: only 'run' when there are no tests (i.e. snapshot dir) - there is no
          // benefit to also running, it's wasteful
          );

  private static TestContainerHook containerHook;

  UseCaseTestExtensions testExtensions = new UseCaseTestExtensions();

  boolean executed = false;

  @BeforeAll
  static void before() {
    var engines = new EngineFactory().createAll();

    containerHook = engines.accept(new TestContainersForTestGoal(), null);
    containerHook.start();
  }

  @BeforeAll
  static void initTestShading() {
    var total = System.getenv("TEST_SHARDING_TOTAL");
    if (ObjectUtils.isEmpty(total)) {
      log.warn("No test sharding");
      return;
    }

    totalShards = Integer.parseInt(total);
    shardIdx = Integer.parseInt(System.getenv("TEST_SHARDING_INDEX"));

    log.warn("Enabled test sharding: total: {} index: {}", totalShards, shardIdx);
  }

  @AfterAll
  static void after() {
    if (containerHook != null) {
      containerHook.teardown();
    }
  }

  @AfterEach
  void tearDown() {
    if (containerHook != null && executed) {
      containerHook.clear();
      executed = false;
    }
  }

  @ParameterizedTest
  @MethodSource("dummyPackageJsonProvider")
  void useCase(UseCaseParam param) {
    assumeThat(disabledScripts.contains(new ScriptCriteria(param.getUseCaseName(), param.goal())))
        .as("Skipping disabled test: %s", param.getUseCaseName())
        .isFalse();

    shard++;
    assumeThat(totalShards == null || shard % totalShards == shardIdx)
        .as(
            "Skipping due to test sharding %s.\n"
                + "shard: %s testShardingTotal: %s testShardingIndex: %s",
            param, shard, totalShards, shardIdx)
        .isTrue();

    executed = true;
    log.info(
        "Testing {} shard: {} testShardingTotal: {} testShardingIndex: {}",
        param.getPackageJsonName(),
        shard,
        totalShards,
        shardIdx);

    var snapshot =
        Snapshot.of(
            FullUseCasesIT.class,
            param.getUseCaseName(),
            param.getPackageJsonName().substring(0, param.getPackageJsonName().length() - 5));

    TestExtension testExtension = testExtensions.create(param.getUseCaseName());
    testExtension.setup();

    Path rootDir = param.packageJsonPath().getParent();

    SqrlScriptExecutor executor =
        SqrlScriptExecutor.builder()
            .goal(param.goal())
            .packageJsonPath(param.packageJsonPath())
            .build();

    AssertStatusHook hook = new AssertStatusHook();
    try {
      executor.execute(hook);
    } catch (Throwable e) {
      if (hook.failure() != null) {
        e.addSuppressed(hook.failure());
      }
      throw e;
    }

    PackageJson packageJson =
        ConfigLoaderUtils.loadResolvedConfig(
            ErrorCollector.root(), rootDir.resolve(SqrlConstants.BUILD_DIR_NAME));

    try {
      TestEngines engines = new EngineFactory().create(packageJson);

      Map<String, String> env = new HashMap<>();
      env.putAll(System.getenv());
      env.putAll(containerHook.getEnv());
      env.put("DATA_PATH", rootDir.resolve("build/deploy/flink/data").toAbsolutePath().toString());
      env.put("UDF_PATH", rootDir.resolve("build/deploy/flink/lib").toAbsolutePath().toString());

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

      // Run the test
      TestEnvContext context =
          TestEnvContext.builder().env(env).rootDir(rootDir).param(param).build();
      // test goal is accomplished by above, but run goal needs extra setup
      DatasqrlRun run = null;
      if (param.goal().equals("run")) {
        try {
          var planDir = context.getRootDir().resolve(SqrlConstants.PLAN_PATH);
          var flinkConfig = TestExecutionEnv.loadInternalTestFlinkConfig(planDir, context.getEnv());
          run = DatasqrlRun.nonBlocking(planDir, packageJson, flinkConfig, context.getEnv());
          TableResult result = run.run();
          long delaySec = packageJson.getTestConfig().getDelaySec();

          int requiredCheckpoints = packageJson.getTestConfig().getRequiredCheckpoints();
          if (delaySec == -1) {
            FlinkOperatorStatusChecker flinkOperatorStatusChecker =
                new FlinkOperatorStatusChecker(
                    result.getJobClient().get().getJobID().toString(), requiredCheckpoints);
            flinkOperatorStatusChecker.run();
          } else {
            Thread.sleep(delaySec * 1000);
          }

          switch (result.getResultKind()) {
            case SUCCESS:
            case SUCCESS_WITH_CONTENT:
              break;
            default:
              fail("Flink job failed with: " + result.getResultKind());
              break;
          }

          try {
            result.getJobClient().get().cancel();
          } catch (Exception expected) {
            // Necessary to get over the error that is thrown when the Flink mini-cluster is stopped
            // already. For some test cases, that will happen, and is expected.
          }
        } catch (Exception e) {
          e.printStackTrace();
          fail("", e);
        }
      }

      engines.accept(
          new TestExecutionEnv(
              param.getPackageJsonName() + ":" + param.goal(), packageJson, rootDir, snapshot),
          context);
      if (run != null) {
        run.cancel();
      }
    } finally {
      containerHook.clear();
    }
    // tear down after we stop flink etc
    testExtension.teardown();

    if (snapshot.hasContent()) {
      snapshot.createOrValidate();
    }
  }

  // FIXME: this is just a temporary method to restrict the execution for 1 migrated usecase
  static Set<UseCaseParam> dummyPackageJsonProvider() {
    var repoPkg = USE_CASES.resolve("repository").resolve("package.json");
    return Set.of(new UseCaseParam(repoPkg, "run"), new UseCaseParam(repoPkg, "test"));
  }

  @SneakyThrows
  static Set<UseCaseParam> packageJsonProvider() {
    var useCasesDir = USE_CASES.toAbsolutePath();

    try (var useCaseStream = Files.list(useCasesDir)) {

      return useCaseStream
          .filter(Files::isDirectory)
          .flatMap(FullUseCasesIT::collectPackageJsonFiles)
          .flatMap(p -> Stream.of(new UseCaseParam(p, "run"), new UseCaseParam(p, "test")))
          .collect(Collectors.toCollection(TreeSet::new));
    }
  }

  /** Collect files that match the {@code package*.json} pattern from a given use case dir. */
  @SneakyThrows
  static Stream<Path> collectPackageJsonFiles(Path useCaseDir) {
    try (var stream = Files.list(useCaseDir)) {
      return stream
          .filter(Files::isRegularFile)
          .filter(
              p -> {
                String n = p.getFileName().toString();
                return n.startsWith("package") && n.endsWith(".json");
              })
          .toList()
          .stream();
    }
  }
}
