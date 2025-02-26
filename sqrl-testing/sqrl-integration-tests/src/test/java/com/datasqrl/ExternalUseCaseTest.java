package com.datasqrl;

import java.nio.file.Path;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

@Disabled
public class ExternalUseCaseTest extends AbstractUseCaseTest {

  public static Path USECASE_DIR;

  protected ExternalUseCaseTest() {
    super(USECASE_DIR);
  }

  @BeforeAll
  public static void readEnvironment() {
    String dirName = System.getenv("DATASQRL_EXTERNAL_TEST_DIR");
    USECASE_DIR = Path.of(dirName);
    System.out.println(USECASE_DIR);
  }

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
}
