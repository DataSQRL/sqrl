// package com.datasqrl;
//
// import static org.junit.jupiter.api.Assertions.fail;
//
// import java.io.File;
// import java.nio.file.Path;
// import java.util.List;
// import java.util.stream.Stream;
// import lombok.extern.slf4j.Slf4j;
// import org.apache.flink.table.api.CompiledPlan;
// import org.junit.jupiter.api.BeforeAll;
// import org.junit.jupiter.api.Disabled;
// import org.junit.jupiter.api.TestInstance;
// import org.junit.jupiter.params.ParameterizedTest;
// import org.junit.jupiter.params.provider.MethodSource;
//
/// **
// * !! This test uses built jars. Remember to build the jars if you're making changes and testing
// */
// @Slf4j
// @TestInstance(TestInstance.Lifecycle.PER_CLASS) // This is to allow the method source to not be
// static
// public class DagPlannerIT {
//
//  // Method to provide the directories as test arguments
//  static Stream<Path> directoryProvider() {
//    Path firstDirPath = Path.of("../sqrl-integration-tests/src/test/resources/dagplanner");
//    Stream<Path> firstDirStream = getDirectoriesStream(firstDirPath);
//
////    Path secondDirPath = Path.of("../sqrl-integration-tests/src/test/resources/usecases/plan");
////    Stream<Path> secondDirStream = getDirectoriesStream(secondDirPath);
//
//    return firstDirStream;
//  }
//
//  private static Stream<Path> getDirectoriesStream(Path rootPath) {
//    File directory = rootPath.toFile();
//    if (directory.exists() && directory.isDirectory()) {
//      File[] sqrlFiles = directory.listFiles(f->
//          f.getName().endsWith(".sqrl") && !f.getName().contains("disabled") &&
// !f.getName().contains("fail"));
//      if (sqrlFiles != null) {
//        return Stream.of(sqrlFiles).map(File::toPath);
//      }
//    }
//    return Stream.empty();
//  }
//
//  static DatasqrlNewRun datasqrlRun;
//
//  @BeforeAll
//  static void beforeAll() {
//    datasqrlRun = new DatasqrlNewRun();
//  }
//
//  List<String> disabled = List.of("tableFunctionsBasic.sqrl",
//      "tableStateJoinTest.sqrl", "tableStreamJoinTest.sqrl",
//      "selectDistinctNestedTest.sqrl", "timestampReassignment.sqrl");
//
//  @ParameterizedTest
//  @MethodSource("directoryProvider")
//  void testCompilePlanOnDirectory(Path directoryPath) {
//    if (disabled.contains(directoryPath.getFileName().toString())){
//      log.warn("Skipping Disabled Test");
//      return;
//    }
//
//    Path root = getProjectRoot(directoryPath);
//
//    SqrlCompiler sqrlCompiler = new SqrlCompiler();
//    sqrlCompiler.execute(directoryPath.getParent(),
//        "compile", directoryPath.getFileName().toString(),
//        "--profile", root.resolve("profiles/default").toString());
//
//    datasqrlRun.setPath(directoryPath.getParent().resolve("build").resolve("plan"));
//    try {
//      CompiledPlan plan = datasqrlRun.compileFlink();
//      // plan.execute().print(); Uncomment if execution is required
//    } catch (Exception e) {
//      fail("Failed to compile plan for directory: " + directoryPath, e);
//    }
//  }
//
//  private Path getProjectRoot(Path directoryPath) {
//    Path currentPath = directoryPath.toAbsolutePath();
//    while (currentPath != null) {
//      if (new File(currentPath.resolve(".git").toString()).exists()) {
//        return currentPath;
//      }
//      currentPath = currentPath.getParent();
//    }
//    return null;
//  }
// }
