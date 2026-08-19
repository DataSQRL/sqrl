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

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.error.ErrorLocation.FileLocation;
import com.datasqrl.loaders.FlinkTableNamespaceObject.FlinkTable;
import com.datasqrl.loaders.schema.SchemaLoader;
import com.datasqrl.planner.parser.SqrlCreateTableStatement;
import java.nio.file.Path;
import java.util.Optional;

public record FlinkTableNamespaceObject(FlinkTable table, SchemaLoader schemaLoader)
    implements TableNamespaceObject<FlinkTable> {

  @Override
  public Name name() {
    return table.name();
  }

  public record FlinkTable(
      Name name,
      String sql,
      Path scriptPath,
      Optional<SqrlCreateTableStatement> sqrlStatement,
      boolean external,
      Optional<String> scriptContent,
      Optional<FileLocation> sourceLocation) {

    public FlinkTable(Name name, String sql, Path scriptPath) {
      this(name, sql, scriptPath, Optional.empty(), true, Optional.empty(), Optional.empty());
    }

    public FlinkTable(
        Name name,
        SqrlCreateTableStatement sqrlStatement,
        Path scriptPath,
        boolean external,
        String scriptContent,
        FileLocation sourceLocation) {
      this(
          name,
          sqrlStatement.toSql(),
          scriptPath,
          Optional.of(sqrlStatement),
          external,
          Optional.of(scriptContent),
          Optional.of(sourceLocation));
    }
  }
}
