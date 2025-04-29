package com.datasqrl;

import static org.junit.Assume.assumeFalse;

import java.nio.file.Path;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

import lombok.SneakyThrows;

/**
 * Compiles the use cases in the test/resources/usecases folder and snapshots the
 * deployment assets
 */
public class UseCaseCompileTest extends AbstractUseCaseTest {

  public static final Path USECASE_DIR = getResourcesDirectory("usecases");

  @Override
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
  

  @Disabled
  @ParameterizedTest
  @ArgumentsSource(UseCaseFiles.class)
  public void runTestCaseByName(Path script, Path graphQlFile, Path packageFile) {
    if (script.toString().endsWith("math-functions.sqrl")
    ) {
    	  super.testUsecase(script, graphQlFile, packageFile);
    } else {
      assumeFalse(true);
    }
  }

}
