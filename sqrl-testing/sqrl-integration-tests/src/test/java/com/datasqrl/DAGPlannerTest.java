package com.datasqrl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Predicate;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

import com.datasqrl.util.SnapshotTest.Snapshot;

public class DAGPlannerTest extends AbstractAssetSnapshotTest {

  public static final Path SCRIPT_DIR = getResourcesDirectory("dagplanner");

  protected DAGPlannerTest() {
    super(SCRIPT_DIR.resolve("deploy-assets"));
  }

  @ParameterizedTest
  @ArgumentsSource(DagPlannerSQRLFiles.class)
  void testScripts(Path script) {
    assertTrue(Files.exists(script));
    var expectFailure = TestNameModifier.of(script)==TestNameModifier.fail;
    this.snapshot = Snapshot.of(getDisplayName(script), getClass());
    var hook = execute(SCRIPT_DIR, "compile", script.getFileName().toString(), "-t", deployDir.toString());
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
    return p -> false;
  }

  static class DagPlannerSQRLFiles extends SqrlScriptArgumentsProvider {
    public DagPlannerSQRLFiles() {
      super(SCRIPT_DIR, false);
    }
  }

}
