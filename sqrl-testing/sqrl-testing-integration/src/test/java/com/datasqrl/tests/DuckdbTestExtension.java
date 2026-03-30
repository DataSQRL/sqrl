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

import com.datasqrl.UseCaseParam;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.stream.Stream;
import lombok.SneakyThrows;

public class DuckdbTestExtension extends AbstractUseCaseExtension {
  private final Path path = Path.of("/tmp/duckdb");

  private void deleteDirectory(Path directory) throws IOException {
    if (!Files.exists(directory)) {
      return;
    }
    try (Stream<Path> paths = Files.walk(directory)) {
      paths
          .sorted(Comparator.reverseOrder())
          .forEach(
              path -> {
                try {
                  Files.delete(path);
                } catch (IOException e) {
                  throw new RuntimeException("Failed to delete " + path, e);
                }
              });
    }
  }

  @SneakyThrows
  public void createDir() {
    Files.createDirectories(path);
  }

  @Override
  protected boolean supports(UseCaseParam param) {
    return param.getUseCaseName().equals("duckdb")
        || param.getUseCaseName().equals("analytics-only");
  }

  @Override
  protected void setup(UseCaseParam param) {
    try {
      deleteDirectory(path);
    } catch (Exception ignored) {
    }

    createDir();
  }

  @Override
  protected void teardown(UseCaseParam param) {
    try {
      deleteDirectory(path);
    } catch (Exception ignored) {
    }
  }
}
