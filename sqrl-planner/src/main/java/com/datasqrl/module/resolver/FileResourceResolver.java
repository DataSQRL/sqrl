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
package com.datasqrl.module.resolver;

import static com.datasqrl.util.NameUtil.namepath2Path;

import com.datasqrl.canonicalizer.NamePath;
import com.google.common.base.Preconditions;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.SneakyThrows;

public class FileResourceResolver implements ResourceResolver {

  Path baseDir;

  public FileResourceResolver(Path baseDir) {
    Preconditions.checkState(Files.isDirectory(baseDir));
    this.baseDir = baseDir;
  }

  @Override
  public String toString() {
    return "FileResourceResolver[" + baseDir + ']';
  }

  @SneakyThrows
  @Override
  public List<Path> loadPath(NamePath namePath) {
    Path path = namepath2Path(baseDir, namePath);

    if (!Files.exists(path)) {
      return List.of();
    }

    return Files.list(path).collect(Collectors.toList());
  }

  @Override
  public Optional<Path> resolveFile(NamePath namePath) {
    var path = namepath2Path(baseDir, namePath);
    if (!Files.exists(path)) {
      return Optional.empty();
    }
    return Optional.of(path);
  }

  @Override
  public Optional<Path> resolveFile(Path relativePath) {
    var path = baseDir.resolve(relativePath);
    if (!Files.exists(path)) {
      return Optional.empty();
    }
    return Optional.of(path);
  }
}
