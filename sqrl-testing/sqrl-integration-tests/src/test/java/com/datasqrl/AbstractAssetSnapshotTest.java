package com.datasqrl;

import com.datasqrl.cmd.AssertStatusHook;
import com.datasqrl.cmd.RootCommand;
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

public abstract class AbstractAssetSnapshotTest {

  public Path buildDir = null;
  public final Path deployDir;
  protected Snapshot snapshot;

  protected AbstractAssetSnapshotTest(Path deployDir) {
    this.deployDir = deployDir;
  }

  @BeforeEach
  public void setup(TestInfo testInfo) throws IOException {
    clearDir(deployDir);
    Files.createDirectories(deployDir);
  }

  @AfterEach
  public void clearDirs() throws IOException {
    clearDir(deployDir);
    clearDir(buildDir);
  }

  public Predicate<Path> getBuildDirFilter() {
    return path -> false;
  }

  public Predicate<Path> getDeployDirFilter() {
    return path -> false;
  }

  public Predicate<Path> getPlanDirFilter() {
    return path -> true;
  }

  protected void clearDir(Path dir) throws IOException {
    if (dir != null && Files.isDirectory(dir)) {
      FileUtils.deleteDirectory(dir.toFile());
    }
  }

  protected void createSnapshot() {
    snapshotFiles(deployDir, getDeployDirFilter());
    snapshotFiles(buildDir, getBuildDirFilter());
    snapshotFiles(buildDir.resolve("plan"), getPlanDirFilter());
    snapshot.createOrValidate();
  }

  protected void createFailSnapshot(String failMessage) {
    snapshot.addContent(failMessage);
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
    this.buildDir = rootDir.resolve("build");
    List<String> argslist = new ArrayList<>(List.of(args));
    AssertStatusHook statusHook = new AssertStatusHook();
    int code = new RootCommand(rootDir,statusHook).getCmd().execute(argslist.toArray(String[]::new));
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
  public static Stream<Path> getSQRLScripts(Path directory) {
    return Files.walk(directory)
        .filter(path -> !Files.isDirectory(path))
        .filter(path -> path.toString().endsWith(SQRL_EXTENSION))
        .filter(path-> !path.toString().contains("/build/"))
        .filter(path -> TestNameModifier.of(path)!=TestNameModifier.disabled)
        .sorted();
  }

  public enum TestNameModifier {
    none, disabled, fail;

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

    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext context) throws IOException {
      return getSQRLScripts(directory).map(Arguments::of);
    }
  }
}