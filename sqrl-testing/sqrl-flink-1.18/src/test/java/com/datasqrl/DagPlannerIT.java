package com.datasqrl;

import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.nio.file.Path;
import java.util.stream.Stream;
import org.apache.flink.table.api.CompiledPlan;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

@TestInstance(TestInstance.Lifecycle.PER_CLASS) // This is to allow the method source to not be static
public class DagPlannerIT {

  // Method to provide the directories as test arguments
  static Stream<Path> directoryProvider() {
    Path firstDirPath = Path.of("../sqrl-integration-tests/src/test/resources/dagplanner");
    Stream<Path> firstDirStream = getDirectoriesStream(firstDirPath);

//    Path secondDirPath = Path.of("../sqrl-integration-tests/src/test/resources/usecases/plan");
//    Stream<Path> secondDirStream = getDirectoriesStream(secondDirPath);

    return firstDirStream;
  }

  private static Stream<Path> getDirectoriesStream(Path rootPath) {
    File directory = rootPath.toFile();
    if (directory.exists() && directory.isDirectory()) {
      File[] sqrlFiles = directory.listFiles(f->
          f.getName().endsWith(".sqrl") && !f.getName().contains("disabled"));
      if (sqrlFiles != null) {
        return Stream.of(sqrlFiles).map(File::toPath);
      }
    }
    return Stream.empty();
  }

  static DatasqrlRun datasqrlRun;

  @BeforeAll
  static void beforeAll() {
    datasqrlRun = new DatasqrlRun();
    datasqrlRun.startKafkaCluster();
  }

  @ParameterizedTest
  @MethodSource("directoryProvider")
  void testCompilePlanOnDirectory(Path directoryPath) {
    SqrlCompiler sqrlCompiler = new SqrlCompiler();
    sqrlCompiler.execute(directoryPath.getParent(),
        "compile", directoryPath.getFileName().toString());

    datasqrlRun.setPath(directoryPath.getParent().resolve("build").resolve("plan"));
    try {
      CompiledPlan plan = datasqrlRun.compileFlink();
      // plan.execute().print(); Uncomment if execution is required
    } catch (Exception e) {
      fail("Failed to compile plan for directory: " + directoryPath, e);
    }
  }
}