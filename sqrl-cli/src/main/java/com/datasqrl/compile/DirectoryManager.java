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
package com.datasqrl.compile;

import java.nio.file.Files;
import java.nio.file.Path;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;

@Slf4j
public class DirectoryManager {

  @SneakyThrows
  public static void prepareTargetDirectory(Path targetDir) {
    if (Files.isDirectory(targetDir)) {
      FileUtils.cleanDirectory(targetDir.toFile());
    } else {
      Files.createDirectories(targetDir);
    }
  }

  @SneakyThrows
  public static void createDirectoriesIfNotExists(Path dir) {
    if (!Files.isDirectory(dir)) {
      Files.createDirectories(dir);
    }
  }
}
