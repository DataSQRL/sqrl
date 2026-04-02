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
import com.datasqrl.util.SnapshotTest.Snapshot;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Predicate;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * JUnit 5 extension that manages temporary directories for snapshot-based compiler tests. Provides
 * directory lifecycle (creation before each test, cleanup after each test), CLI execution, temp
 * package file management, and snapshot assembly.
 */
public class CliCompileTestExtension implements BeforeEachCallback, AfterEachCallback {

  private final @Nullable Path outputDirName;

  @Getter private Path outputDir;
  @Getter private Path buildDir;
  @Getter private Path planDir;
  @Getter @Setter private Snapshot snapshot;
  @Getter private Path tempPackage;
  private Path tempRoot;

  /** Creates an extension with no separate output directory. */
  public CliCompileTestExtension() {
    this(null);
  }

  /**
   * Creates an extension with a named output directory that will be created as a subdirectory of
   * the temp root.
   *
   * @param outputDirName relative name for the output directory, or null for no output directory
   */
  public CliCompileTestExtension(@Nullable Path outputDirName) {
    this.outputDirName = outputDirName;
  }

  @Override
  public void beforeEach(ExtensionContext context) throws Exception {
    tempRoot = Files.createTempDirectory("sqrl-snapshot-test-");
    if (outputDirName != null) {
      outputDir = tempRoot.resolve(outputDirName);
      Files.createDirectories(outputDir);
    } else {
      outputDir = null;
    }
    buildDir = null;
    planDir = null;
    snapshot = null;
    tempPackage = null;
  }

  @Override
  public void afterEach(ExtensionContext context) throws Exception {
    if (tempPackage != null) {
      Files.deleteIfExists(tempPackage);
    }
    clearDir(buildDir);
    clearDir(tempRoot);
  }

  public AssertStatusHook execute(Path rootDir, String... args) {
    return execute(rootDir, List.of(args));
  }

  /**
   * Executes the compiler command specified by the provided argument list. Sets {@link #buildDir}
   * and {@link #planDir} based on the root directory.
   */
  public AssertStatusHook execute(Path rootDir, List<String> argsList) {
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

  /**
   * Writes a temporary packages.json by replacing a template substring with the script filename.
   */
  @SneakyThrows
  public void writeTempPackage(Path script, String templateToReplace) {
    var templatePkg = script.getParent().resolve("package.json");
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

  /**
   * Creates the snapshot from selected files in the build, output, and plan directories using the
   * provided filter predicates.
   */
  public void createSnapshot(
      Predicate<Path> buildDirFilter,
      Predicate<Path> outputDirFilter,
      Predicate<Path> planDirFilter) {
    SnapshotTestSupport.snapshotFiles(snapshot, buildDir, 1, buildDirFilter);
    SnapshotTestSupport.snapshotFiles(snapshot, outputDir, outputDirFilter);
    SnapshotTestSupport.snapshotFiles(snapshot, planDir, planDirFilter);
    snapshot.createOrValidate();
  }

  public void createMessageSnapshot(String messages) {
    snapshot.addContent(messages);
    snapshot.createOrValidate();
  }

  private void clearDir(Path dir) throws IOException {
    if (dir != null && Files.isDirectory(dir)) {
      FileUtils.deleteDirectory(dir.toFile());
    }
  }
}
