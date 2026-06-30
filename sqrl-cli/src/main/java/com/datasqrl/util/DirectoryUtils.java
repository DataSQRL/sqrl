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
package com.datasqrl.util;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;

@Slf4j
public class DirectoryUtils {

  public static final Path EMPTY_PATH = Path.of("");

  @SneakyThrows
  public static void prepareTargetDirectory(Path targetDir) {
    if (Files.isDirectory(targetDir)) {
      FileUtils.cleanDirectory(targetDir.toFile());
    } else {
      Files.createDirectories(targetDir);
    }
  }

  @SneakyThrows
  public static Path deepestCommonSubDir(List<Path> paths) {
    if (paths == null || paths.isEmpty()) {
      throw new IllegalArgumentException("The provided path list must not be null or empty");
    }

    return paths.stream()
        .map(Path::getParent)
        .map(path -> path == null ? EMPTY_PATH : path.normalize())
        .reduce(
            (left, right) -> {
              while (!right.startsWith(left)) {
                left = left.getParent();
                if (left == null) {
                  return EMPTY_PATH;
                }
              }
              return left;
            })
        .orElse(EMPTY_PATH);
  }
}
