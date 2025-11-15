/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasqrl;

import static org.assertj.core.api.Assertions.assertThat;

import com.datasqrl.cli.AssertStatusHook;
import com.datasqrl.cli.DatasqrlCli;
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;

/**
 * Abstract base class for snapshot testing, i.e. comparing the produced results against previously
 * snapshotted results. If they are equal, the test succeeds. If the snapshot is new or they
 * mismatch, it fails.
 *
 * <p>This test class provides the basic infrastructure for snapshot testing. Sub-classes implement
 * the actual invocation of the test and filters for which files produced by the compiler are added
 * to the snapshot.
 */
public abstract class AbstractAssetSnapshotTest {

  public final Path outputDir; // Additional output directory for test assets

  public Path buildDir = null; // Build directory for the compiler
  public Path planDir = null; // Plan directory for the compiler, within the build directory
  protected Snapshot snapshot;

  protected AbstractAssetSnapshotTest(Path outputDir) {
    this.outputDir = outputDir;
  }

  @BeforeEach
  public void setup(TestInfo testInfo) throws IOException {
    clearDir(outputDir);
    if (outputDir != null) {
      Files.createDirectories(outputDir);
    }
  }

  @AfterEach
  public void clearDirs() throws IOException {
    clearDir(outputDir);
    clearDir(buildDir);
  }

  /**
   * Selects the files from the build directory that are added to the snapshot
   *
   * @return
   */
  public Predicate<Path> getBuildDirFilter() {
    return path -> false;
  }

  /**
   * Selects the files from the configured output directory that are added to the snapshot
   *
   * @return
   */
  public Predicate<Path> getOutputDirFilter() {
    return path -> false;
  }

  /**
   * Selects the files from the plan directory that are added to the snapshot
   *
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

  /** Creates the snapshot from the selected files */
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
    if (path == null || !Files.isDirectory(path)) return;
    List<Path> paths = new ArrayList<>();
    Files.walkFileTree(
        path,
        new SimpleFileVisitor<>() {
          @Override
          public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
              throws IOException {
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
    Collections.sort(paths); // Create deterministic order
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
   *
   * @param rootDir
   * @param argsList
   * @return
   */
  protected AssertStatusHook execute(Path rootDir, List<String> argsList) {
    this.buildDir = rootDir.resolve(SqrlConstants.BUILD_DIR_NAME).toAbsolutePath();
    this.planDir =
        buildDir
            .resolve(SqrlConstants.DEPLOY_DIR_NAME)
            .resolve(SqrlConstants.PLAN_DIR)
            .toAbsolutePath();
    var statusHook = new AssertStatusHook();
    var code =
        new DatasqrlCli(rootDir, statusHook, true)
            .getCmd()
            .execute(argsList.toArray(String[]::new));
    if (statusHook.isSuccess() && code != 0) {
      assertThat(code).isEqualTo(0);
    }
    return statusHook;
  }

  public static String getDisplayName(Path path) {
    if (path == null) {
      return "";
    }
    var filename = path.getFileName().toString();
    var length = filename.indexOf('.');
    if (length < 0) {
      length = filename.length();
    }
    return filename.substring(0, length);
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
    return getFilesByPattern(script.getParent(), scriptName + "*.graphqls");
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

  public static Stream<Path> getSQRLScripts(Path directory, boolean includeFail) {
    return getSQRLScripts(directory, true, includeFail);
  }

  @SneakyThrows
  public static Stream<Path> getSQRLScripts(
      Path directory, boolean includeNested, boolean includeFail) {
    return Files.walk(directory, includeNested ? Integer.MAX_VALUE : 1)
        .filter(path -> !Files.isDirectory(path))
        .filter(path -> path.toString().endsWith(SQRL_EXTENSION))
        .filter(path -> !path.getFileName().toString().equalsIgnoreCase("sources" + SQRL_EXTENSION))
        .filter(path -> !path.toString().contains("/build/"))
        .filter(
            path -> {
              TestNameModifier mod = TestNameModifier.of(path);
              return mod == TestNameModifier.none
                  || (includeFail
                      && (mod == TestNameModifier.fail || mod == TestNameModifier.warn));
            })
        .sorted();
  }

  /**
   * We extract the TestNameModifier from the end of the filename and use it to set expectations for
   * the test.
   */
  public enum TestNameModifier {
    none, // Normal test that we expect to succeed
    disabled, // Disabled test that is not executed
    warn, // This test should succeed but issue warnings that we are testing
    fail; // This test is expected to fail and we are testing the error message

    public static TestNameModifier of(String filename) {
      if (Strings.isNullOrEmpty(filename)) {
        return none;
      }
      var name = FileUtil.separateExtension(filename).getLeft().toLowerCase();
      return Arrays.stream(TestNameModifier.values())
          .filter(mod -> name.endsWith(mod.name()))
          .findFirst()
          .orElse(none);
    }

    public static TestNameModifier of(Path file) {
      if (file == null) {
        return none;
      }
      return TestNameModifier.of(file.getFileName().toString());
    }
  }

  @AllArgsConstructor
  public abstract static class SqrlScriptArgumentsProvider implements ArgumentsProvider {

    Path directory;
    boolean includeFail;

    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext context)
        throws IOException {
      return getSQRLScripts(directory, false, includeFail)
          .sorted(Comparator.comparing(c -> c.toFile().getName().toLowerCase()))
          .map(Arguments::of);
    }
  }
}
