package com.datasqrl;

import com.datasqrl.cmd.RootCommand;
import com.datasqrl.cmd.StatusHook;
import com.datasqrl.util.FileUtil;
import com.datasqrl.util.SnapshotTest.Snapshot;
import com.datasqrl.util.StringUtil;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;

public abstract class AbstractAssetSnapshotTest {

  protected Path buildDir = null;
  protected final Path deployDir;
  protected final StatusHook hook;
  protected Snapshot snapshot;

  protected AbstractAssetSnapshotTest(Path deployDir, StatusHook hook) {
    this.deployDir = deployDir;
    this.hook = hook;
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

  protected void clearDir(Path dir) throws IOException {
    if (dir != null && Files.isDirectory(dir)) {
      FileUtils.deleteDirectory(dir.toFile());
    }
  }

  protected void createSnapshot() {
    snapshotFiles(buildDir, getBuildDirFilter());
    snapshotFiles(deployDir, getDeployDirFilter());
    snapshot.createOrValidate();
  }

  @SneakyThrows
  private void snapshotFiles(Path path, Predicate<Path> predicate) {
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

  protected int execute(Path rootDir, String... args) {
    buildDir = rootDir.resolve("build");
    List<String> argslist = new ArrayList<>(List.of(args));
    return new RootCommand(rootDir,hook).getCmd().execute(argslist.toArray(String[]::new));
  }


  public static String getDisplayName(Path path) {
    return FileUtil.separateExtension(path).getKey();
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
  private static final String DISABLED_FLAG = "disabled";

  @SneakyThrows
  public static Stream<Path> getSQRLScripts(Path directory) {
    return Files.walk(directory)
        .filter(path -> !Files.isDirectory(path))
        .filter(path -> path.toString().endsWith(SQRL_EXTENSION))
        .filter(path-> !path.toString().contains("/build/"))
        .filter(path -> !StringUtil.removeFromEnd(path.getFileName().toString(), SQRL_EXTENSION).endsWith(DISABLED_FLAG))
        .sorted();
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