package com.datasqrl;

import java.nio.file.Path;
import java.util.function.Predicate;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

import com.datasqrl.actions.WriteDag;

/**
 * Compiles the use cases in the test/resources/usecases folder and snapshots the
 * deployment assets
 */
public class DAGWriterJsonTest extends AbstractUseCaseTest {

  public static final Path USECASE_DIR = getResourcesDirectory("usecases/clickstream");

  protected DAGWriterJsonTest() {
    super(USECASE_DIR);
  }

  @Override
@ParameterizedTest
  @ArgumentsSource(UseCaseFiles.class)
  void testUsecase(Path script, Path graphQlFile, Path packageFile) {
    super.testUsecase(script, graphQlFile, packageFile);
  }

  static class UseCaseFiles extends SqrlScriptsAndLocalPackages {
    public UseCaseFiles() {
      super(USECASE_DIR, false);
    }
  }

  @Override
public Predicate<Path> getBuildDirFilter() {
    return path -> path.getFileName().toString().endsWith(WriteDag.EXPLAIN_JSON_FILENAME);
  }

  @Override
public Predicate<Path> getPlanDirFilter() {
    return path -> false;
  }

}
