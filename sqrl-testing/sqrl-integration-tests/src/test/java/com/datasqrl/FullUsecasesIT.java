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

import static org.junit.Assume.assumeFalse;
import static org.junit.jupiter.api.Assertions.fail;

import com.datasqrl.cmd.AssertStatusHook;
import com.datasqrl.config.PackageJson;
import com.datasqrl.config.SqrlConfigCommons;
import com.datasqrl.config.SqrlConstants;
import com.datasqrl.config.TestRunnerConfiguration;
import com.datasqrl.engines.TestContainersForTestGoal;
import com.datasqrl.engines.TestContainersForTestGoal.TestContainerHook;
import com.datasqrl.engines.TestEngine.EngineFactory;
import com.datasqrl.engines.TestEngines;
import com.datasqrl.engines.TestExecutionEnv;
import com.datasqrl.engines.TestExecutionEnv.TestEnvContext;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.tests.TestExtension;
import com.datasqrl.tests.UseCaseTestExtensions;
import com.datasqrl.util.FlinkOperatorStatusChecker;
import com.datasqrl.util.SnapshotTest.Snapshot;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.ToString;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.assertj.core.api.Assumptions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

@Slf4j
@ExtendWith(MiniClusterExtension.class)
public class FullUsecasesIT {
  private static final Path RESOURCES = Path.of("src/test/resources");
  private static final Path USE_CASES = RESOURCES.resolve("usecases");

  private Snapshot snapshot;

  @Value
  class ScriptCriteria {
    String name;
    String goal;
  }

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
          new ScriptCriteria(
              "temporal-join.sqrl",
              "run") // TODO: only 'run' when there are no tests (i.e. snapshot dir) - there is no
                     // benefit to also running, it's wasteful
          );

  static final Path PROJECT_ROOT = Path.of(System.getProperty("user.dir"));

  private static TestContainerHook containerHook;

  UseCaseTestExtensions testExtensions = new UseCaseTestExtensions();

  boolean executed = false;

  @AfterEach
  public void tearDown() throws Exception {
    if (containerHook != null && executed) {
      containerHook.clear();
      executed = false;
    }
  }

  @BeforeAll
  public static void before() {
    var engines = new EngineFactory().createAll();

    containerHook = engines.accept(new TestContainersForTestGoal(), null);
    containerHook.start();
  }

  @AfterAll
  public static void after() {
    if (containerHook != null) {
      containerHook.teardown();
    }
  }

  @AllArgsConstructor
  @Getter
  @ToString
  public static class UseCaseTestParameter implements Comparable<UseCaseTestParameter> {

    String parentDirectory;
    String goal;

    @ToString.Include(rank = 0)
    String useCaseName;

    @ToString.Include(rank = 1)
    String sqrlFileName;

    String graphqlFileName;
    String testName;
    String testPath;
    String optionalParam; // Can be null
    String packageJsonPath; // Can be null

    public UseCaseTestParameter cloneWithGoal(String goal) {
      return new UseCaseTestParameter(
          parentDirectory,
          goal,
          useCaseName,
          sqrlFileName,
          graphqlFileName,
          testName,
          testPath,
          optionalParam,
          packageJsonPath);
    }

    private static final Comparator<UseCaseTestParameter> ORDER =
        Comparator.comparing(UseCaseTestParameter::getSqrlFileName)
            .thenComparing(UseCaseTestParameter::getGoal)
            .thenComparing(UseCaseTestParameter::toString);

    @Override
    public int compareTo(UseCaseTestParameter o) {
      return ORDER.compare(this, o);
    }
  }

  static int shard;
  static Integer testShardingTotal;
  static int testShardingIndex;

  @BeforeAll
  public static void initTestShading() throws Exception {
    var total = System.getenv("TEST_SHARDING_TOTAL");
    if (ObjectUtils.isEmpty(total)) {
      log.warn("No test sharding");
      return;
    }

    testShardingTotal = Integer.parseInt(total);
    testShardingIndex = Integer.parseInt(System.getenv("TEST_SHARDING_INDEX"));

    log.warn("Enabled test sharding: total: {} index: {}", testShardingTotal, testShardingIndex);
  }

  @SneakyThrows
  @ParameterizedTest
  @MethodSource("useCaseProvider")
  public void testUseCase(UseCaseTestParameter param) {
    if (disabledScripts.contains(new ScriptCriteria(param.getSqrlFileName(), param.getGoal()))) {
      log.warn("Skipping disabled test:" + param.getSqrlFileName());
      Assumptions.assumeThat(false)
          .as("Skipping disabled test: %s", param.getSqrlFileName())
          .isTrue();
    }

    shard++;
    if (testShardingTotal != null && shard % testShardingTotal != testShardingIndex) {
      log.warn(
          "Skipping due to test sharding {} shard: {} testShardingTotal: {} testShardingIndex: {}",
          param.sqrlFileName,
          shard,
          testShardingTotal,
          testShardingIndex);
      Assumptions.assumeThat(false)
          .as(
              "Skipping due to test sharding %s.\n"
                  + "shard: %s testShardingTotal: %s testShardingIndex: %s",
              param, shard, testShardingTotal, testShardingIndex)
          .isTrue();
    } else {
      log.warn(
          "Testing sharding {} shard: {} testShardingTotal: {} testShardingIndex: {}",
          param.sqrlFileName,
          shard,
          testShardingTotal,
          testShardingIndex);
    }

    executed = true;

    this.snapshot =
        Snapshot.of(
            FullUsecasesIT.class,
            param.testName,
            param.getSqrlFileName().substring(0, param.getSqrlFileName().length() - 5));
    TestExtension testExtension = testExtensions.create(param.getTestName());
    testExtension.setup();

    Path rootDir = USE_CASES.resolve(param.getUseCaseName());

    SqrlScriptExecutor executor =
        SqrlScriptExecutor.builder()
            .rootDir(rootDir)
            .goal(param.getGoal())
            .script(param.getSqrlFileName())
            .graphql(param.getGraphqlFileName())
            .testSuffix(param.getTestName())
            .testPath(param.getTestPath())
            .packageJsonPath(param.getPackageJsonPath())
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
        SqrlConfigCommons.fromFilesPackageJson(
            ErrorCollector.root(),
            List.of(
                rootDir.resolve(SqrlConstants.BUILD_DIR_NAME).resolve(SqrlConstants.PACKAGE_JSON)));

    try {
      TestEngines engines = new EngineFactory().create(packageJson);

      Map<String, String> env = new HashMap<>();
      env.putAll(System.getenv());
      env.putAll(containerHook.getEnv());
      env.put("DATA_PATH", rootDir.resolve("build/deploy/flink/data").toAbsolutePath().toString());
      env.put("UDF_PATH", rootDir.resolve("build/deploy/flink/lib").toAbsolutePath().toString());

      // Log test run
      log.info(
          "The test parameters\n"
              + "Test name: "
              + param.getTestName()
              + "\n"
              + "Test path: "
              + rootDir
              + "\n"
              + "Test sqrl file: "
              + param.getSqrlFileName()
              + "\n"
              + "Test graphql file: "
              + param.getGraphqlFileName()
              + "\n");

      // Run the test
      TestEnvContext context =
          TestEnvContext.builder().env(env).rootDir(rootDir).param(param).build();
      // test goal is accomplished by above, but run goal needs extra setup
      DatasqrlRun run = null;
      if (param.getGoal().equals("run")) {
        try {
          run =
              new DatasqrlRun(
                  context.getRootDir().resolve(SqrlConstants.PLAN_PATH), context.getEnv());
          TableResult result = run.run(false);
          long delaySec =
              packageJson
                  .getTestConfig()
                  .flatMap(TestRunnerConfiguration::getDelaySec)
                  .map(Duration::getSeconds)
                  .orElse((long) -1);
          int requiredCheckpoints =
              packageJson
                  .getTestConfig()
                  .flatMap(TestRunnerConfiguration::getRequiredCheckpoints)
                  .orElse(0);
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
          } catch (Exception e) {
          }
        } catch (Exception e) {
          e.printStackTrace();
          fail(e);
        }
      }

      engines.accept(
          new TestExecutionEnv(
              param.getSqrlFileName() + ":" + param.goal, packageJson, rootDir, snapshot),
          context);
      if (run != null) {
        run.stop();
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

  static int testNo = 0;

  @ParameterizedTest
  @MethodSource("useCaseProvider")
  @Disabled
  public void runTestNumber(UseCaseTestParameter param) {
    var testToExecute = 45;
    testNo++;
    System.out.println(testNo + ":" + param);
    if (testToExecute == testNo) {
      testUseCase(param);
    } else {
      assumeFalse(true);
    }
  }

  @ParameterizedTest
  @MethodSource("useCaseProvider")
  @Disabled
  public void printUseCaseNumbers(UseCaseTestParameter param) {
    testNo++;
    System.out.println(testNo + ":" + param);
  }

  @ParameterizedTest
  @MethodSource("useCaseProvider")
  @Disabled
  public void runTestCaseByName(UseCaseTestParameter param) {
    if (param.sqrlFileName.equals("temporal-join.sqrl") && param.goal.equals("test")) {
      testUseCase(param);
    } else {
      assumeFalse(true);
    }
  }

  static Set<UseCaseTestParameter> useCaseProvider() throws Exception {
    var useCasesDir = USE_CASES;
    Set<UseCaseTestParameter> params = new TreeSet<>();

    Files.list(useCasesDir)
        .filter(Files::isDirectory)
        .forEach(
            dir -> {
              var useCaseName = dir.getFileName().toString();

              try (var stream = Files.newDirectoryStream(dir)) {
                for (Path file : stream) {
                  var fileName = file.getFileName().toString();

                  if (!fileName.endsWith(".sqrl")) {
                    continue;
                  }

                  var testName = fileName.substring(0, fileName.length() - 5);
                  var graphql = testName + ".graphqls";
                  var packageJson = "package-" + testName + ".json";
                  var testPath = "tests-" + testName;
                  if (!file.getParent().resolve(graphql).toFile().exists()) {
                    graphql = null;
                  }
                  if (!file.getParent().resolve(packageJson).toFile().exists()) {
                    packageJson = null;
                  }
                  if (!file.getParent().resolve(testPath).toFile().exists()) {
                    testPath = null;
                  }
                  var useCaseTestParameter =
                      new UseCaseTestParameter(
                          "usecases",
                          "test",
                          useCaseName,
                          fileName,
                          graphql,
                          testName,
                          testPath,
                          null,
                          packageJson);
                  params.add(useCaseTestParameter);
                  params.add(useCaseTestParameter.cloneWithGoal("run"));
                }
              } catch (Exception e) {
                fail("Unable to process use case: " + useCaseName, e);
              }
            });

    return params;
  }
}
