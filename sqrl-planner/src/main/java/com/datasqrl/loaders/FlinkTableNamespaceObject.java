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
import com.datasqrl.io.tables.TableSchema;
import com.datasqrl.loaders.FlinkTableNamespaceObject.FlinkTable;
import com.datasqrl.module.TableNamespaceObject;
import java.nio.file.Path;
import java.util.Optional;
import lombok.Getter;
import lombok.Value;

@Getter
public class FlinkTableNamespaceObject implements TableNamespaceObject<FlinkTable> {

  private final FlinkTable table;

  public FlinkTableNamespaceObject(FlinkTable table) {
    this.table = table;
  }

  @Override
  public Name getName() {
    return table.getName();
  }

  @Value
  public static class FlinkTable {
    Name name;
    String flinkSQL;
    Path flinkSqlFile;
    Optional<TableSchema> schema;
  }
}
