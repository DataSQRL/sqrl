package com.datasqrl;

import com.datasqrl.actions.DagWriter;
import java.nio.file.Path;
import java.util.function.Predicate;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

/**
 * Creates a DAG plan for a single use case test (clickstream)
 */
public class DAGWriterJsonTest extends AbstractUseCaseTest {

  public static final Path USECASE_DIR = getResourcesDirectory("usecases/clickstream");

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

  public Predicate<Path> getBuildDirFilter() {
    return path -> path.getFileName().toString().endsWith(DagWriter.EXPLAIN_JSON_FILENAME);
  }

  public Predicate<Path> getPlanDirFilter() {
    return path -> false;
  }

}
