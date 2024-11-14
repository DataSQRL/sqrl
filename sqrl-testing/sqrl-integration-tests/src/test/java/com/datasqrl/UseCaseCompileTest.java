package com.datasqrl;

import java.nio.file.Path;
import lombok.SneakyThrows;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

/**
 * Compiles the use cases in the test/resources/usecases folder and snapshots the
 * deployment assets
 */
public class UseCaseCompileTest extends AbstractUseCaseTest {

  public static final Path USECASE_DIR = getResourcesDirectory("usecases");

  protected UseCaseCompileTest() {
    super(USECASE_DIR);
  }

  @SneakyThrows
  @ParameterizedTest
  @ArgumentsSource(UseCaseFiles.class)
  void testUsecase(Path script, Path graphQlFile, Path packageFile) {
    super.testUsecase(script, graphQlFile, packageFile);
  }

  static class UseCaseFiles extends SqrlScriptsAndLocalPackages {
    public UseCaseFiles() {
      super(USECASE_DIR, true);
    }
  }
}
