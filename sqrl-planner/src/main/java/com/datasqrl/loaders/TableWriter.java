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
package com.datasqrl.loaders;

import static com.datasqrl.loaders.ModuleLoaderImpl.TABLE_FILE_SUFFIX;

import com.datasqrl.planner.tables.FlinkTableBuilder;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import lombok.NonNull;

public class TableWriter {

  public TableWriter() {}

  public static Path writeToFile(
      @NonNull Path destinationDir, @NonNull String filename, @NonNull FlinkTableBuilder table) {
    var tableConfigFile = destinationDir.resolve(filename + TABLE_FILE_SUFFIX);
    try {
      var sql = table.buildSql(false).toString() + ";";
      Files.writeString(tableConfigFile, sql);
      return tableConfigFile;
    } catch (IOException e) {
      throw new RuntimeException(
          "Could not write table [%s] to file [%s]"
              .formatted(table.getTableName(), tableConfigFile));
    }
  }
}
