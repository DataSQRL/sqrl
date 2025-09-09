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
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.function.Predicate;
import javax.annotation.Nullable;
import lombok.SneakyThrows;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;

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
  protected Path tempPackage;

  protected AbstractAssetSnapshotTest(Path outputDir) {
    this.outputDir = outputDir;
  }

  @BeforeEach
  public void setup(TestInfo testInfo) throws IOException {
    clearDir(outputDir);
    if (outputDir != null) {
      Files.createDirectories(outputDir);
    }
    if (tempPackage != null) {
      Files.deleteIfExists(tempPackage);
    }
  }

  @AfterEach
  public void cleanup() throws IOException {
    clearDir(outputDir);
    clearDir(buildDir);
    if (tempPackage != null) {
      Files.deleteIfExists(tempPackage);
    }
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
    snapshotFiles(buildDir, 1, getBuildDirFilter());
    snapshotFiles(outputDir, getOutputDirFilter());
    snapshotFiles(planDir, getPlanDirFilter());
    snapshot.createOrValidate();
  }

  protected void createMessageSnapshot(String messages) {
    snapshot.addContent(messages);
    snapshot.createOrValidate();
  }

  protected void writeTempPackage(Path script, String templateToReplace) {
    writeTempPackage(script, null, templateToReplace);
  }

  @SneakyThrows
  protected void writeTempPackage(
      Path script, @Nullable String packageName, String templateToReplace) {
    var pkgName = packageName != null ? packageName : "package.json";
    var templatePkg = script.getParent().resolve(pkgName);
    assertThat(templatePkg).isRegularFile();

    var content = Files.readString(templatePkg);
    assertThat(content)
        .as("Template package must contain given template string")
        .contains(templateToReplace);
    content = content.replace(templateToReplace, script.getFileName().toString());

    if (tempPackage != null) {
      Files.deleteIfExists(tempPackage);
    }

    tempPackage = Files.createTempFile(script.getParent(), "test", "package.json");
    tempPackage.toFile().deleteOnExit();

    Files.writeString(tempPackage, content);
  }

  @SneakyThrows
  private void snapshotFiles(Path path, Predicate<Path> predicate) {
    snapshotFiles(path, Integer.MAX_VALUE, predicate);
  }

  @SneakyThrows
  private void snapshotFiles(Path path, int maxDepth, Predicate<Path> predicate) {
    if (path == null || !Files.isDirectory(path)) return;
    List<Path> paths = new ArrayList<>();
    Files.walkFileTree(
        path,
        EnumSet.noneOf(FileVisitOption.class),
        maxDepth,
        new SimpleFileVisitor<>() {
          @Override
          public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
            if (predicate.test(file)) {
              paths.add(file);
            }
            return FileVisitResult.CONTINUE;
          }

          @Override
          public FileVisitResult visitFileFailed(Path file, IOException exc) {
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

  /**
   * We extract the TestNameModifier from the end of the filename and use it to set expectations for
   * the test.
   */
  public enum TestNameModifier {
    none, // Normal test that we expect to succeed
    compile, // Test that expected to get compiled, but not executed
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
}
