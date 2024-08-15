package com.datasqrl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datasqrl.cmd.AssertStatusHook;
import com.datasqrl.util.SnapshotTest.Snapshot;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Predicate;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

public class DAGPlannerTest extends AbstractAssetSnapshotTest {

  public static final Path SCRIPT_DIR = getResourcesDirectory("dagplanner");

  protected DAGPlannerTest() {
    super(SCRIPT_DIR.resolve("deploy-assets"));
  }

  @ParameterizedTest
  @ArgumentsSource(DagPlannerSQRLFiles.class)
  void testScripts(Path script) {
    assertTrue(Files.exists(script));
    boolean expectFailure = TestNameModifier.of(script)==TestNameModifier.fail;
    this.snapshot = Snapshot.of(getDisplayName(script), getClass());
    Path planDir = SCRIPT_DIR.resolve("plan").resolve(getDisplayName(script));
    AssertStatusHook hook = execute(SCRIPT_DIR, planDir, "compile", script.getFileName().toString(), "-t", deployDir.toString(),
        "--plan", planDir.toString(),
        "--profile", "../../../../../../profiles/default");
    assertEquals(expectFailure, hook.isFailed(), hook.getFailMessage());
    if (expectFailure) {
      createFailSnapshot(hook.getFailMessage());
    } else {
      createSnapshot();
    }
  }

  @Override
  public Predicate<Path> getBuildDirFilter() {
    return file -> {
      switch (file.getFileName().toString()) {
        case "pipeline_explain.txt": return true;
      }
      return false;
    };
  }

  @Override
  public Predicate<Path> getDeployDirFilter() {
    return (p)->false;
  }

  static class DagPlannerSQRLFiles extends SqrlScriptArgumentsProvider {
    public DagPlannerSQRLFiles() {
      super(SCRIPT_DIR);
    }
  }

}
