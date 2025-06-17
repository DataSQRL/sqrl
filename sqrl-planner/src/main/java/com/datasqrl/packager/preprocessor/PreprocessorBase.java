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
package com.datasqrl.packager.preprocessor;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.loaders.DataSource;
import com.datasqrl.util.NameUtil;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;
import lombok.SneakyThrows;

public abstract class PreprocessorBase implements Preprocessor {

  @SneakyThrows
  protected static boolean tableExists(Path basePath, String tableName) {
    String expectedFileName =
        NameUtil.namepath2Path(basePath, NamePath.of(tableName + DataSource.TABLE_FILE_SUFFIX))
            .getFileName()
            .toString();

    try (Stream<Path> paths = Files.list(basePath)) {
      return paths.anyMatch(
          path ->
              Files.isRegularFile(path)
                  && path.getFileName().toString().equalsIgnoreCase(expectedFileName));
    }
  }
}
