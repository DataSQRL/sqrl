package com.datasqrl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datasqrl.cmd.AssertStatusHook;
import com.datasqrl.util.SnapshotTest.Snapshot;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Predicate;
import org.junit.jupiter.api.Test;

public class DAGPlannerTest extends AbstractAssetSnapshotTest {

  public static final Path SCRIPT_DIR = getResourcesDirectory("dagplanner");

  protected DAGPlannerTest() {
    super(SCRIPT_DIR.resolve("deploy-assets"));
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
  public Predicate<Path> getDeployDirFilter() {
    return (p) -> false;
  }

  private void testScript(Path script) {
    System.out.println("PATH: " + script);
    assertTrue(Files.exists(script));
    boolean expectFailure = TestNameModifier.of(script) == TestNameModifier.fail;
    this.snapshot = Snapshot.of(getDisplayName(script), getClass());
    AssertStatusHook hook =
        execute(
            SCRIPT_DIR,
            "compile",
            script.getFileName().toString(),
            "-t",
            deployDir.toString(),
            "--profile",
            "../../../../../../profiles/default");
    assertEquals(expectFailure, hook.isFailed(), hook.getFailMessage());
    if (expectFailure) {
      createFailSnapshot(hook.getFailMessage());
    } else {
      createSnapshot();
    }
  }

  @Test
  void testAddingSimpleColumns() {
    testScript(SCRIPT_DIR.resolve("addingSimpleColumns.sqrl"));
  }

  @Test
  void testAggregateWithMaxTimestamp() {
    testScript(SCRIPT_DIR.resolve("aggregateWithMaxTimestamp.sqrl"));
  }

  @Test
  void testComplexConditions() {
    testScript(SCRIPT_DIR.resolve("complexConditions.sqrl"));
  }

  @Test
  void testConditions() {
    testScript(SCRIPT_DIR.resolve("conditions.sqrl"));
  }

  @Test
  void testDistinctTest() {
    testScript(SCRIPT_DIR.resolve("distinctTest.sqrl"));
  }

  @Test
  void testExportStreamTest() {
    testScript(SCRIPT_DIR.resolve("exportStreamTest.sqrl"));
  }

  @Test
  void testFunctionTest() {
    testScript(SCRIPT_DIR.resolve("functionTest.sqrl"));
  }

  @Test
  void testImportWithNaturalTimestampTest() {
    testScript(SCRIPT_DIR.resolve("importWithNaturalTimestampTest.sqrl"));
  }

  @Test
  void testImports() {
    testScript(SCRIPT_DIR.resolve("imports.sqrl"));
  }

  @Test
  void testInnerNonIntervalJoinExport() {
    testScript(SCRIPT_DIR.resolve("innerNonIntervalJoinExport.sqrl"));
  }

  @Test
  void testJoinTimestampPropagationTest() {
    testScript(SCRIPT_DIR.resolve("joinTimestampPropagationTest.sqrl"));
  }

  @Test
  void testJoinTypesTest() {
    testScript(SCRIPT_DIR.resolve("joinTypesTest.sqrl"));
  }

  @Test
  void testJsonInDatabaseTest() {
    testScript(SCRIPT_DIR.resolve("jsonInDatabaseTest.sqrl"));
  }

  @Test
  void testJsonInStreamTest() {
    testScript(SCRIPT_DIR.resolve("jsonInStreamTest.sqrl"));
  }

  @Test
  void testNestedAggregationAndSelfJoinTest() {
    testScript(SCRIPT_DIR.resolve("nestedAggregationAndSelfJoinTest.sqrl"));
  }

  @Test
  void testNestedTableWithUnnest() {
    testScript(SCRIPT_DIR.resolve("nestedTableWithUnnest.sqrl"));
  }

  @Test
  void testNowAggregateTest() {
    testScript(SCRIPT_DIR.resolve("nowAggregateTest.sqrl"));
  }

  @Test
  void testNowFilterTest() {
    testScript(SCRIPT_DIR.resolve("nowFilterTest.sqrl"));
  }

  @Test
  void testSelectDistinctNestedTest() {
    testScript(SCRIPT_DIR.resolve("selectDistinctNestedTest.sqrl"));
  }

  @Test
  void testSelectDistinctTest() {
    testScript(SCRIPT_DIR.resolve("selectDistinctTest.sqrl"));
  }

  @Test
  void testSetOperationRelationTest() {
    testScript(SCRIPT_DIR.resolve("setOperationRelationTest.sqrl"));
  }

  @Test
  void testStaticDataTest() {
    testScript(SCRIPT_DIR.resolve("staticDataTest.sqrl"));
  }

  @Test
  void testStreamAggregateTest() {
    testScript(SCRIPT_DIR.resolve("streamAggregateTest.sqrl"));
  }

  @Test
  void testStreamStateAggregateTest() {
    testScript(SCRIPT_DIR.resolve("streamStateAggregateTest.sqrl"));
  }

  @Test
  void testTableFunctionsBasic() {
    testScript(SCRIPT_DIR.resolve("tableFunctionsBasic.sqrl"));
  }

  @Test
  void testTableIntervalJoinTest() {
    testScript(SCRIPT_DIR.resolve("tableIntervalJoinTest.sqrl"));
  }

  @Test
  void testTableStateJoinTest() {
    testScript(SCRIPT_DIR.resolve("tableStateJoinTest.sqrl"));
  }

  @Test
  void testTableStreamJoinTest() {
    testScript(SCRIPT_DIR.resolve("tableStreamJoinTest.sqrl"));
  }

  @Test
  void testTableTemporalJoinWithTimeFilterTest() {
    testScript(SCRIPT_DIR.resolve("tableTemporalJoinWithTimeFilterTest.sqrl"));
  }

  @Test
  void testTestExecutionTypeHint() {
    testScript(SCRIPT_DIR.resolve("testExecutionTypeHint.sqrl"));
  }

  @Test
  void testTimestampColumnDefinitionWithPropagation() {
    testScript(SCRIPT_DIR.resolve("timestampColumnDefinitionWithPropagation.sqrl"));
  }

  @Test
  void testTopNTest() {
    testScript(SCRIPT_DIR.resolve("topNTest.sqrl"));
  }

  @Test
  void testUnionWithTimestamp() {
    testScript(SCRIPT_DIR.resolve("unionWithTimestamp.sqrl"));
  }

  @Test
  void testUnionWithoutTimestamp() {
    testScript(SCRIPT_DIR.resolve("unionWithoutTimestamp.sqrl"));
  }
}
