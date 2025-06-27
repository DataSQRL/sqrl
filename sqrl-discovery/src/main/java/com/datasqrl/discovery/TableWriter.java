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
package com.datasqrl.discovery;

import com.datasqrl.planner.tables.FlinkTableBuilder;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import lombok.NonNull;

public class TableWriter {
  public static final String TABLE_FILE_SUFFIX = ".table.sql";

  public TableWriter() {}

  public Collection<Path> writeToFile(
      @NonNull Path destinationDir, @NonNull FlinkTableBuilder table) throws IOException {
    var tableConfigFile = destinationDir.resolve(table.getTableName() + TABLE_FILE_SUFFIX);
    var sql = table.buildSql(false).toString() + ";";
    Files.writeString(tableConfigFile, sql);
    return List.of(tableConfigFile);
  }
}
