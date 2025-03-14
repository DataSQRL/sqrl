package com.datasqrl;

import static com.datasqrl.UseCasesIT.getProjectRoot;

import com.datasqrl.cmd.AssertStatusHook;
import com.datasqrl.cmd.RootCommand;
import com.datasqrl.config.SqrlConstants;
import com.datasqrl.util.FileUtil;
import com.datasqrl.util.SnapshotTest.Snapshot;
import com.google.common.base.Strings;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;

/**
 * Abstract base class for snapshot testing, i.e. comparing the produced results
 * against previously snapshotted results. If they are equal, the test succeeds.
 * If the snapshot is new or they mismatch, it fails.
 *
 * This test class provides the basic infrastructure for snapshot testing.
 * Sub-classes implement the actual invocation of the test and filters for which
 * files produced by the compiler are added to the snapshot.
 */
public abstract class AbstractAssetSnapshotTest {

  public final Path outputDir; //Additional output directory for test assets

  public Path buildDir = null; //Build directory for the compiler
  public Path planDir = null; //Plan directory for the compiler, within the build directory
  protected Snapshot snapshot;

  protected AbstractAssetSnapshotTest(Path outputDir) {
    this.outputDir = outputDir;
  }

  @BeforeEach
  public void setup(TestInfo testInfo) throws IOException {
    clearDir(outputDir);
    if (outputDir!=null) Files.createDirectories(outputDir);
  }

  @AfterEach
  public void clearDirs() throws IOException {
    clearDir(outputDir);
    clearDir(buildDir);
  }

  /**
   * Selects the files from the build directory that are added to the snapshot
   * @return
   */
  public Predicate<Path> getBuildDirFilter() {
    return path -> false;
  }

  /**
   * Selects the files from the configured output directory that are added to the snapshot
   * @return
   */
  public Predicate<Path> getOutputDirFilter() {
    return path -> false;
  }

  /**
   * Selects the files from the plan directory that are added to the snapshot
   * @return
   */
  public Predicate<Path> getPlanDirFilter() {
    return path -> true;
  }

  protected void clearDir(Path dir) throws IOException {
    if (dir != null && Files.isDirectory(dir)) {
      FileUtils.deleteDirectory(dir.toFile());
    }
  }

  /**
   * Creates the snapshot from the selected files
   */
  protected void createSnapshot() {
    snapshotFiles(buildDir, getBuildDirFilter());
    snapshotFiles(outputDir, getOutputDirFilter());
    snapshotFiles(planDir, getPlanDirFilter());
    snapshot.createOrValidate();
  }

  protected void createMessageSnapshot(String messages) {
    snapshot.addContent(messages);
    snapshot.createOrValidate();
  }

  @SneakyThrows
  private void snapshotFiles(Path path, Predicate<Path> predicate) {
    if (path==null || !Files.isDirectory(path)) return;
    List<Path> paths = new ArrayList<>();
    Files.walkFileTree(path, new SimpleFileVisitor<>() {
      @Override
      public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
        if (predicate.test(file)) {
          paths.add(file);
        }
        return FileVisitResult.CONTINUE;
      }

      @Override
      public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
        return FileVisitResult.CONTINUE;
      }
    });
    Collections.sort(paths); //Create deterministic order
    for (Path file : paths) {
      try {
        snapshot.addContent(Files.readString(file), file.getFileName().toString());
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  protected AssertStatusHook execute(Path rootDir, String... args) {
    return execute(rootDir, new ArrayList<>(List.of(args)));
  }

  /**
   * Executes the compiler command specified by the provided argument list
   * @param rootDir
   * @param argsList
   * @return
   */
  protected AssertStatusHook execute(Path rootDir, List<String> argsList) {
    this.buildDir = rootDir.resolve(SqrlConstants.BUILD_DIR_NAME);
    this.planDir = buildDir.resolve(SqrlConstants.DEPLOY_DIR_NAME).resolve(SqrlConstants.PLAN_DIR);
    argsList.add("--profile");
    argsList.add(getProjectRoot().resolve("profiles/default").toString());
    AssertStatusHook statusHook = new AssertStatusHook();
    int code = new RootCommand(rootDir,statusHook).getCmd().execute(argsList.toArray(String[]::new));
    if (statusHook.isSuccess()) Assertions.assertEquals(0, code);
    return statusHook;
  }


  public static String getDisplayName(Path path) {
    if (path==null) return "";
    String filename = path.getFileName().toString();
    int length = filename.indexOf('.');
    if (length<0) length = filename.length();
    return filename.substring(0,length);
  }

  public static Path getResourcesDirectory(String subdir) {
    return Path.of("src", "test", "resources").resolve(subdir);
  }

  @SneakyThrows
  public static List<Path> getPackageFiles(Path dir) {
    return getFilesByPattern(dir, "package*.json");
  }

  @SneakyThrows
  public static List<Path> getScriptGraphQLFiles(Path script) {
    String scriptName = FileUtil.separateExtension(script).getKey();
    return getFilesByPattern(script.getParent(), scriptName + ".*graphqls");
  }

  @SneakyThrows
  private static List<Path> getFilesByPattern(Path dir, String pattern) {
    List<Path> result = new ArrayList<>();
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, pattern)) {
      for (Path entry : stream) {
        if (Files.isRegularFile(entry)) {
          result.add(entry);
        }
      }
    }
    return result;
  }

  private static final String SQRL_EXTENSION = ".sqrl";

  @SneakyThrows
  public static Stream<Path> getSQRLScripts(Path directory, boolean includeFail) {
    return Files.walk(directory)
        .filter(path -> !Files.isDirectory(path))
        .filter(path -> path.toString().endsWith(SQRL_EXTENSION))
        .filter(path-> !path.toString().contains("/build/"))
        .filter(path -> {
          TestNameModifier mod = TestNameModifier.of(path);
          return mod==TestNameModifier.none || (includeFail && mod==TestNameModifier.fail);
        })
        .sorted();
  }

  /**
   * We extract the TestNameModifier from the end of the filename and use it
   * to set expectations for the test.
   */
  public enum TestNameModifier {
    none, //Normal test that we expect to succeed
    disabled, //Disabled test that is not executed
    warn, //This test should succeed but issue warnings that we are testing
    fail; //This test is expected to fail and we are testing the error message

    public static TestNameModifier of(String filename) {
      if (Strings.isNullOrEmpty(filename)) return none;
      String name = FileUtil.separateExtension(filename).getLeft().toLowerCase();
      return Arrays.stream(TestNameModifier.values())
          .filter(mod -> name.endsWith(mod.name()))
          .findFirst().orElse(none);
    }

    public static TestNameModifier of(Path file) {
      if (file==null) return none;
      return TestNameModifier.of(file.getFileName().toString());
    }

  }

  @AllArgsConstructor
  public abstract static class SqrlScriptArgumentsProvider implements ArgumentsProvider {

    Path directory;
    boolean includeFail;

    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext context) throws IOException {
      return getSQRLScripts(directory, includeFail)
          .sorted(Comparator.comparing(c -> c.toFile().getName().toLowerCase())).map(Arguments::of);
    }
  }
}