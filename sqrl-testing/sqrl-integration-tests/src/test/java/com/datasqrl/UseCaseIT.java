package com.datasqrl;

import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.nio.file.Path;
import java.util.Optional;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.testcontainers.containers.DockerComposeContainer;

public class UseCaseIT extends AbstractUseCaseTest {

  public static final Path USECASE_DIR = getResourcesDirectory("usecases");

  protected UseCaseIT() {
    super(USECASE_DIR);
  }

  @ParameterizedTest
  @ArgumentsSource(UseCaseFiles.class)
  void testUsecase(Path script, Path graphQlFile, Path packageFile) {
    super.testUsecase(script, graphQlFile, packageFile,
        Optional.of(Path.of("../../profile-1.16")));

    try (DockerComposeContainer<?> environment = new DockerComposeContainer<>(deployDir
        .resolve("docker-compose.yml").toFile())
        .withBuild(true)) {
      environment.start();
      System.out.println("Docker Compose services started");
    } catch (Exception e) {
      fail(e);
    }
  }

  static class UseCaseFiles extends SqrlScriptsAndLocalPackages {
    public UseCaseFiles() {
      super(USECASE_DIR);
    }
  }



}
