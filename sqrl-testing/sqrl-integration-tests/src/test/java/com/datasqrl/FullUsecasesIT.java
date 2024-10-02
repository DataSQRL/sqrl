package com.datasqrl;

import static org.junit.jupiter.api.Assertions.fail;

import com.datasqrl.cmd.AssertStatusHook;
import com.datasqrl.config.PackageJson;
import com.datasqrl.config.SqrlConfigCommons;
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
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.ToString;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.table.api.ResultKind;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.junit.AfterClass;
import org.junit.BeforeClass;
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
  private static final Path RESOURCES = Paths.get("src/test/resources");
  private static final Path USE_CASES = RESOURCES.resolve("usecases");

  private Snapshot snapshot;

  @Value
  class ScriptCriteria {
    String name;
    String goal;
  }

  List<ScriptCriteria> disabledScripts = List.of(
//      new ScriptCriteria("sensors-mutation.sqrl", "test"), //flaky
//      new ScriptCriteria("sensors-mutation.sqrl", "run"), //build server issues
//      new ScriptCriteria("conference.sqrl", "test"), //minicluster has different results??
//      new ScriptCriteria("conference.sqrl", "run"), //build server issues
      new ScriptCriteria("duckdb.sqrl", "test"), //fails in build server
      new ScriptCriteria("duckdb.sqrl", "run"), //fails in build server
//      new ScriptCriteria("snowflake.sqrl", "test"), //fails in build server
//      new ScriptCriteria("snowflake.sqrl", "run"), //fails in build server
      new ScriptCriteria("sensors-full.sqrl", "test"), //flaky (too much data)
      new ScriptCriteria("sensors-full.sqrl", "run"), //flaky (too much data)
//      new ScriptCriteria("postgres-log.sqrl", "test"), // invalid flink sql
//      new ScriptCriteria("postgres-log.sqrl", "run"), // invalid flink sql
//      new ScriptCriteria("metrics.sqrl", "run"), // Unknown engine: postgres_log
      new ScriptCriteria("seedshop-extended.sqrl", "test"), // CustomerPromotionTest issue
      new ScriptCriteria("seedshop-extended.sqrl", "run") // CustomerPromotionTest issue
////      new ScriptCriteria("snowflake.sqrl", "run"), // sometimes snowflake doesn't respond fast enough
//      new ScriptCriteria("src.sqrl", "test"), // file location incorrect
//      new ScriptCriteria("src.sqrl", "run"), // file location incorrect
//      new ScriptCriteria("src-import.sqrl", "test"), // file location incorrect
//      new ScriptCriteria("src-import.sqrl", "run"), // file location incorrect
//      new ScriptCriteria("patient-sensor.sqrl", "run"), // file location incorrect
//      new ScriptCriteria("patient-sensor.sqrl", "test") // file location incorrect
  );

  static final Path PROJECT_ROOT = Paths.get(System.getProperty("user.dir"));

  private static TestContainerHook containerHook;

  UseCaseTestExtensions testExtensions = new UseCaseTestExtensions();


  @AfterEach
  public void tearDown() throws Exception {
    if (containerHook != null) {
      containerHook.clear();
    }
  }

  @BeforeAll
  public static void before() {
    TestEngines engines = new EngineFactory()
        .createAll();

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
  public static class UseCaseTestParameter {

    String parentDirectory;
    String goal;
    String useCaseName;
    String sqrlFileName;
    String graphqlFileName;
    String testName;
    String testPath;
    String optionalParam; // Can be null
    String packageJsonPath; // Can be null

    public UseCaseTestParameter cloneWithGoal(String goal) {
      return new UseCaseTestParameter(parentDirectory, goal,
          useCaseName, sqrlFileName, graphqlFileName, testName,
          testPath, optionalParam, packageJsonPath);
    }
  }

  @SneakyThrows
  @ParameterizedTest
  @MethodSource("useCaseProvider")
  public void testUseCase(UseCaseTestParameter param) {
    if (disabledScripts.contains(new ScriptCriteria(param.getSqrlFileName(), param.getGoal()))) {
      log.warn("Skipping disabled test:" + param.getSqrlFileName());
      return;
    }
    this.snapshot = new Snapshot(param.testName, param.getSqrlFileName().substring(0, param.getSqrlFileName().length()-5), new StringBuilder());
    TestExtension testExtension = testExtensions.create(param.getTestName());
    testExtension.setup();

    Path rootDir = USE_CASES.resolve(param.getUseCaseName());

    SqrlScriptExecutor executor = SqrlScriptExecutor.builder()
        .rootDir(rootDir)
        .goal(param.getGoal())
        .script(param.getSqrlFileName())
        .graphql(param.getGraphqlFileName())
        .testSuffix(param.getTestName())
        .testPath(param.getTestPath())
        .packageJsonPath(param.getPackageJsonPath())
        .build();

    executor.execute(new AssertStatusHook());

    PackageJson packageJson = SqrlConfigCommons.fromFilesPackageJson(ErrorCollector.root(),
        List.of(rootDir.resolve("build").resolve("package.json")));


    try {
      TestEngines engines = new EngineFactory()
          .create(packageJson);

      Map<String, String> env = new HashMap<>();
      env.putAll(System.getenv());
      env.put("EXECUTION_MODE", "local");
      env.putAll(containerHook.getEnv());
      env.put("DATA_PATH", rootDir.resolve("build/deploy/flink/data").toAbsolutePath().toString());

      //Run the test
      TestEnvContext context = TestEnvContext.builder()
          .env(env)
          .rootDir(rootDir)
          .param(param)
          .build();
      //test goal is accomplished by above, but run goal needs extra setup
      DatasqrlRun run = null;
      if (param.getGoal().equals("run")) {
        try {
          run = new DatasqrlRun(context.getRootDir().resolve("build/plan"),
              context.getEnv());
          TableResult result = run.run(false);
         long delaySec = packageJson.getTestConfig().flatMap(TestRunnerConfiguration::getDelaySec)
              .map(Duration::getSeconds)
              .orElse((long) -1);
          if (delaySec == -1 ) {
            FlinkOperatorStatusChecker flinkOperatorStatusChecker = new FlinkOperatorStatusChecker(
                result.getJobClient().get().getJobID().toString());
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
          } catch (Exception e) {}
        } catch (Exception e) {
          e.printStackTrace();
          fail(e);
        }
      }

      engines.accept(new TestExecutionEnv(param.getUseCaseName(), packageJson, rootDir, snapshot),
          context);
      if (run != null) {
        run.stop();
      }
    } finally {
      containerHook.clear();
    }
    //tear down after we stop flink etc
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
    int i = 30;
    testNo++;
    System.out.println(testNo + ":" + param);
    if (i == testNo) {
      testUseCase(param);
    }
  }

  static List<UseCaseTestParameter> useCaseProvider() throws Exception {
    Path useCasesDir = USE_CASES;
    List<UseCaseTestParameter> params = new ArrayList<>();

    Files.list(useCasesDir).filter(Files::isDirectory).forEach(dir -> {
      String useCaseName = dir.getFileName().toString();

      try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
        for (Path file : stream) {
          String fileName = file.getFileName().toString();

          if (!fileName.endsWith(".sqrl")) {
            continue;
          }

          String testName = fileName.substring(0, fileName.length() - 5);
          String graphql = testName + ".graphqls";
          String packageJson = "package-" + testName + ".json";
          String testPath = "tests-" + testName;
          if (!file.getParent().resolve(graphql).toFile().exists()) {
            graphql = null;
          }
          if (!file.getParent().resolve(packageJson).toFile().exists()) {
            packageJson = null;
          }
          if (!file.getParent().resolve(testPath).toFile().exists()) {
            testPath = null;
          }
          UseCaseTestParameter useCaseTestParameter = new UseCaseTestParameter(
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
        e.printStackTrace();
      }
    });

    return params;
  }

}
