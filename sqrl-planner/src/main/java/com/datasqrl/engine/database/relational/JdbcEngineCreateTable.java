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
package com.datasqrl.engine.database.relational;

import com.datasqrl.engine.database.EngineCreateTable;
import com.datasqrl.engine.log.MutationEngine.MutationCreateTable;
import com.datasqrl.planner.analyzer.TableAnalysis;
import com.datasqrl.planner.tables.FlinkTableBuilder;
import com.datasqrl.planner.tags.TableTags;
import java.util.Map;
import org.apache.calcite.rel.type.RelDataType;

/**
 * For the JDBC database engines, we just keep track of the schema data so we can plan them later in
 * the plan method.
 *
 * @param tags the effective tags of the table, those of its table analysis unless a mutation table
 *     sets them with {@link #withTags}
 */
public record JdbcEngineCreateTable(
    String tableName,
    String logicalName,
    Role role,
    FlinkTableBuilder table,
    RelDataType datatype,
    TableAnalysis tableAnalysis,
    TableTags tags)
    implements EngineCreateTable, MutationCreateTable {

  public JdbcEngineCreateTable(
      String tableName,
      String logicalName,
      Role role,
      FlinkTableBuilder table,
      RelDataType datatype,
      TableAnalysis tableAnalysis) {
    this(
        tableName,
        logicalName,
        role,
        table,
        datatype,
        tableAnalysis,
        tableAnalysis == null ? TableTags.EMPTY : tableAnalysis.getTags());
  }

  public enum Role {
    MUTATION,
    MATERIALIZATION
  }

  public JdbcEngineCreateTable withRole(Role role) {
    return new JdbcEngineCreateTable(
        tableName, logicalName, role, table, datatype, tableAnalysis, tags);
  }

  @Override
  public MutationCreateTable withValueType(RelDataType inputValueType) {
    return new JdbcEngineCreateTable(
        tableName, logicalName, role, table, inputValueType, tableAnalysis, tags);
  }

  @Override
  public MutationCreateTable withTags(TableTags tags) {
    return new JdbcEngineCreateTable(
        tableName, logicalName, role, table, datatype, tableAnalysis, tags);
  }

  @Override
  public Map<String, String> getConfig() {
    return Map.of();
  }
}
