package com.datasqrl;

import static org.assertj.core.api.Assertions.assertThat;

import com.datasqrl.util.ArgumentsProviders;
import com.datasqrl.util.SnapshotTest;
import java.nio.file.Path;
import java.util.function.Predicate;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

public class PredicatePushdownRulesTest extends AbstractAssetSnapshotTest {

  public static final Path SCRIPT_DIR = getResourcesDirectory("predicate-pushdown-rules");

  PredicatePushdownRulesTest() {
    super(SCRIPT_DIR.resolve("out"));
  }

  @ParameterizedTest
  @ArgumentsSource(SqrlFiles.class)
  void testDefaultRules(Path script) {
    compile("package.json", script);
  }

  @ParameterizedTest
  @ArgumentsSource(SqrlFiles.class)
  void testLimitedRules(Path script) {
    compile("package-limited.json", script);
  }

  private void compile(String pkg, Path script) {
    assertThat(script).isRegularFile();
    writeTempPackage(script, pkg, "__SQRL_SCRIPT__");

    this.snapshot =
        SnapshotTest.Snapshot.of(getDisplayName(script) + '-' + getDisplayName(pkg), getClass());
    var hook =
        execute(
            SCRIPT_DIR,
            "compile",
            tempPackage.getFileName().toString(),
            "-t",
            outputDir.getFileName().toString());

    assertThat(hook.isFailed()).as(hook.getMessages()).isFalse();
    createSnapshot();
  }

  @Override
  public Predicate<Path> getOutputDirFilter() {
    return path -> path.getFileName().toString().equals("flink-compiled-plan.json");
  }

  static class SqrlFiles extends ArgumentsProviders.SqrlScriptProvider {
    SqrlFiles() {
      super(SCRIPT_DIR, true);
    }
  }
}
