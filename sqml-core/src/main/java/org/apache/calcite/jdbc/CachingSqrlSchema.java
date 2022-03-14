/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.jdbc;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.calcite.schema.SqrlSchema;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.validate.ListScope;
import org.apache.calcite.sql.validate.TableAliasScope;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A no-op wrapper for SimpleCalciteSchema
 *
 * Note: It -could- be possible to treat table paths as arbitrarily nested schemas here except we
 *  cannot deduce the last element of a path to return a table. Alternatively, it -could- be
 *  possible to treat the DOT operator in a table name as a function.
 */
public class CachingSqrlSchema extends org.apache.calcite.jdbc.SimpleCalciteSchema {
  private final Map<String, TableEntry> tableMap = new HashMap<>();
  private final SqrlSchema sqrlSchema;
  private ListScope scope = null;

  public CachingSqrlSchema(SqrlSchema schema) {
    super(null, schema, "");
    this.sqrlSchema = schema;
  }

  /**
   * Caches the table between validation and relation building. Tables in SQRL are dynamic therefore
   *  we must persist its contents between calls. A new schema object should be created to resolve a
   *  'clean' schema.
   */
  @Override
  protected @Nullable TableEntry getImplicitTable(String tableName, boolean caseSensitive) {
    if (tableMap.containsKey(tableName)) {
      return tableMap.get(tableName);
    }

    Table table = schema.getTable(tableName);
    /*
     * To refactor:
     * The self operator ('_') is sometimes an alias. Check the aliases to determine
     * if we should create the table. Because you can refer to a relative table path
     * at any time, we need to check if we are treating it as an alias or as a full path,
     * so we can create the right table.
     */
    if (scope != null) {
      TableAliasScope s = new TableAliasScope(getSqrlSchema(), this.scope);
      Optional<Table> scopedTable = s.getOrMaybeCreateTable(tableName);
      if (scopedTable.isPresent()) {
        if (table != null && !tableName.startsWith("_")) {
          throw new RuntimeException("Ambiguous table name: " + tableName);
        }
        //We need to treat "_" as both an alias or a full path
        table = scopedTable.get();
      }
    }

    if (table != null) {
      TableEntry tableEntry = tableEntry(tableName, table);
      tableMap.put(tableName, tableEntry);
      return tableEntry;
    }
    return null;
  }

  public SqrlSchema getSqrlSchema() {
    return sqrlSchema;
  }

  public void setScope(ListScope scope) {
    this.scope = scope;
  }
}
