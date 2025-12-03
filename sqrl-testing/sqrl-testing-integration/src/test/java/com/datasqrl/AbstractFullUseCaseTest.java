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

import static com.datasqrl.config.SqrlConstants.BUILD_DIR_NAME;
import static org.assertj.core.api.Assertions.fail;

import com.datasqrl.cli.AssertStatusHook;
import com.datasqrl.cli.DatasqrlTest;
import com.datasqrl.cli.output.DefaultOutputFormatter;
import com.datasqrl.config.PackageJson;
import com.datasqrl.config.SqrlConstants;
import com.datasqrl.engines.TestContainersForTestGoal;
import com.datasqrl.engines.TestContainersForTestGoal.TestContainerHook;
import com.datasqrl.engines.TestEngine.EngineFactory;
import com.datasqrl.env.GlobalEnvironmentStore;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.tests.TestExtension;
import com.datasqrl.tests.UseCaseTestExtensions;
import com.datasqrl.util.ConfigLoaderUtils;
import com.datasqrl.util.SnapshotTest.Snapshot;
import com.datasqrl.util.SqrlScriptExecutor;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;

/** Abstract base class to run a full test on a given project in form of {@link UseCaseParam}. */
@Slf4j
abstract class AbstractFullUseCaseTest {

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

  void fullUseCaseTest(UseCaseParam param) {
    log.info("Testing {}", param.getPackageJsonName());

    var snapshot =
        Snapshot.of(
            AbstractFullUseCaseTest.class,
            param.getUseCaseName(),
            param.getPackageJsonName().substring(0, param.getPackageJsonName().length() - 5));

    TestExtension testExtension = testExtensions.create(param.getUseCaseName());
    try {
      testExtension.setup();

      // Execute compile phase
      SqrlScriptExecutor executor = new SqrlScriptExecutor(param.packageJsonPath(), param.goal());
      AssertStatusHook hook = new AssertStatusHook();
      try {
        executor.execute(hook);
      } catch (Throwable e) {
        if (hook.failure() != null) {
          e.addSuppressed(hook.failure());
        }
        throw e;
      }

      Path rootDir = param.packageJsonPath().getParent();

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

      // Execute the test phase manually via DatasqrlTest
      PackageJson packageJson =
          ConfigLoaderUtils.loadResolvedConfig(
              ErrorCollector.root(), rootDir.resolve(BUILD_DIR_NAME));

      var env = new HashMap<>(containerHook.getEnv());
      env.putAll(System.getenv());
      env.putAll(GlobalEnvironmentStore.getAll());
      env.put("DATA_PATH", rootDir.resolve("build/deploy/flink/data").toAbsolutePath().toString());
      env.put("UDF_PATH", rootDir.resolve("build/deploy/flink/lib").toAbsolutePath().toString());

      var planDir =
          rootDir
              .resolve(SqrlConstants.BUILD_DIR_NAME)
              .resolve(SqrlConstants.DEPLOY_DIR_NAME)
              .resolve(SqrlConstants.PLAN_DIR);
      var flinkConfig = loadInternalTestFlinkConfig(planDir, env);
      var formatter = new DefaultOutputFormatter(true);
      var test = new DatasqrlTest(rootDir, planDir, packageJson, flinkConfig, env, formatter);
      try {
        var run = test.run();
        if (run != 0) {
          fail(
              "Test runner returned error code while running test case '%s'. Check above for failed snapshot tests (in red) or exceptions"
                  .formatted(param.getUseCaseName()));
        }
      } catch (Exception e) {
        fail(
            "Test runner threw exception while running test case '%s'"
                .formatted(param.getUseCaseName()),
            e);
      }

    } finally {
      testExtension.teardown();
      containerHook.clear();
    }

    if (snapshot.hasContent()) {
      snapshot.createOrValidate();
    }
  }

  @SneakyThrows
  static Configuration loadInternalTestFlinkConfig(Path planDir, Map<String, String> env) {
    var flinkConfig = ConfigLoaderUtils.loadFlinkConfig(planDir);

    flinkConfig.set(DeploymentOptions.TARGET, "local");
    if (env.get("FLINK_RESTART_STRATEGY") != null) {
      flinkConfig.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
      flinkConfig.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 0);
      flinkConfig.set(
          RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofSeconds(5));
    }

    flinkConfig.removeConfig(CheckpointingOptions.CHECKPOINTS_DIRECTORY);
    flinkConfig.removeConfig(CheckpointingOptions.SAVEPOINT_DIRECTORY);

    return flinkConfig;
  }
}
