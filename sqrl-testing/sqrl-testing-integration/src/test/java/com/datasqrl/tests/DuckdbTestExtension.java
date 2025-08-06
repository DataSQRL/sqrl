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
package com.datasqrl.tests;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import lombok.SneakyThrows;

public class DuckdbTestExtension implements TestExtension {
  Path path = Path.of("/tmp/duckdb");

  private void deleteDirectory(Path directory) throws IOException {
    Files.walk(directory)
        .sorted(
            (path1, path2) -> path2.compareTo(path1)) // Sort in reverse order to delete files first
        .forEach(
            path -> {
              try {
                Files.delete(path);
              } catch (IOException e) {
                throw new RuntimeException("Failed to delete " + path, e);
              }
            });
  }

  @SneakyThrows
  public void createDir() {

    Files.createDirectories(path);
  }

  @Override
  public void setup() {
    try {
      deleteDirectory(path);
    } catch (Exception e) {
    }

    createDir();
  }

  @Override
  public void teardown() {
    try {
      deleteDirectory(path);
    } catch (Exception e) {
    }
  }
}
