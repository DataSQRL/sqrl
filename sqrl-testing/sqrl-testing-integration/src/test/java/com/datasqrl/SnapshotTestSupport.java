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
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

/**
 * Static utility methods for snapshot testing. Provides file collection, display name generation,
 * and test name modifiers.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
@Slf4j
public final class SnapshotTestSupport {

  /**
   * Collects files from a directory tree, filters them, and appends their contents to a snapshot.
   */
  @SneakyThrows
  public static void snapshotFiles(Snapshot snapshot, Path path, Predicate<Path> predicate) {
    snapshotFiles(snapshot, path, Integer.MAX_VALUE, predicate);
  }

  /**
   * Same as {@link #snapshotFiles(Snapshot, Path, Predicate)} but with a configurable max depth.
   */
  @SneakyThrows
  public static void snapshotFiles(
      Snapshot snapshot, Path path, int maxDepth, Predicate<Path> predicate) {
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
        log.warn("Failed to read file {}", file, e);
      }
    }
  }

  public static String getNestedDisplayName(Path path) {
    var dirsStr =
        path.getParent() == null ? "none" : path.getParent().toString().replaceAll("/", "-");
    var fileStr = getDisplayName(path.getFileName());
    return dirsStr + '-' + fileStr;
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
