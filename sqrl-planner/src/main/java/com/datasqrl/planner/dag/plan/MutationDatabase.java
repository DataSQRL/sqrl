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
package com.datasqrl.planner.dag.plan;

import com.datasqrl.calcite.type.TypeCompatibility;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.graphql.exec.FlinkExecFunction;
import com.datasqrl.planner.Sqrl2FlinkSQLTranslator;
import com.datasqrl.planner.Sqrl2FlinkSQLTranslator.ParsedRelDataTypeResult;
import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.flink.sql.parser.ddl.SqlTableColumn;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
public record MutationDatabase(List<Table> tables) {

  public static MutationDatabase from(Collection<MutationTable> mutationTables) {
    var tables =
        mutationTables.stream()
            .map(
                mutTbl -> {
                  var tblBuilder = mutTbl.getTableBuilder();
                  var columns =
                      tblBuilder.getColumnList().getList().stream()
                          .map(
                              node -> {
                                if (node instanceof SqlTableColumn column) {
                                  var name = column.getName().toString();
                                  var entireColumn = column.toString();
                                  var spec = entireColumn.substring(entireColumn.indexOf(' ') + 1);
                                  return new ColumnDefinition(name, spec);
                                }
                                return new ColumnDefinition("", node.toString());
                              })
                          .toList();
                  var definition =
                      new TableDefinition(
                          columns,
                          tblBuilder.getPrimaryKey().orElse(List.of()),
                          tblBuilder.getPartition());
                  return new Table(
                      mutTbl.getName().getCanonical(),
                      mutTbl.getStage().name(),
                      tblBuilder.buildSql(false).toString(),
                      definition,
                      mutTbl.getCreateTable().getConfig(),
                      mutTbl.getDocumentation().orElse(""));
                })
            .toList();
    return new MutationDatabase(tables);
  }

  public boolean isBackwardsCompatible(
      MutationDatabase compareDb, Sqrl2FlinkSQLTranslator env, ErrorCollector errors) {
    var compareTablesByName =
        compareDb.tables().stream().collect(Collectors.toMap(Table::canonicalName, t -> t));

    var compatible = true;
    for (var table : tables) {
      var compareTable = compareTablesByName.get(table.canonicalName());
      if (compareTable == null) {
        continue;
      }

      if (!table.engine().equals(compareTable.engine())) {
        errors.warn(
            "Table '%s' engine changed from '%s' to '%s'",
            table.canonicalName(), compareTable.engine(), table.engine());
        compatible = false;
      }

      if (!table.definition().primaryKey().equals(compareTable.definition().primaryKey())) {
        errors.warn(
            "Table '%s' primary key changed from %s to %s",
            table.canonicalName(),
            compareTable.definition().primaryKey(),
            table.definition().primaryKey());
        compatible = false;
      }

      if (!table.definition().partitionKey().equals(compareTable.definition().partitionKey())) {
        errors.warn(
            "Table '%s' partition key changed from %s to %s",
            table.canonicalName(),
            compareTable.definition().partitionKey(),
            table.definition().partitionKey());
        compatible = false;
      }

      List<ParsedRelDataTypeResult> newSchema = env.parse2RelDataType(table.createTableSql());
      List<ParsedRelDataTypeResult> oldSchema =
          env.parse2RelDataType(compareTable.createTableSql());

      var oldFieldsByName =
          oldSchema.stream()
              .collect(Collectors.toMap(r -> r.field().getName(), Function.identity()));

      for (var newField : newSchema) {
        var oldField = oldFieldsByName.get(newField.field().getName());
        if (oldField == null) {
          continue;
        }

        if (!TypeCompatibility.isBackwardsCompatible(
            newField.field().getType(), oldField.field().getType())) {
          errors.warn(
              "Table '%s' field '%s' type is not backwards compatible: '%s' -> '%s'",
              table.canonicalName(),
              newField.field().getName(),
              oldField.field().getType(),
              newField.field().getType());
          compatible = false;
        }

        if (newField.metadata().isPresent() || oldField.metadata().isPresent()) {
          if (!Objects.equals(newField.metadata(), oldField.metadata())) {
            errors.warn(
                "Table '%s' field '%s' metadata changed from '%s' to '%s'",
                table.canonicalName(),
                newField.field().getName(),
                oldField.metadata().orElse(null),
                newField.metadata().orElse(null));
            compatible = false;
          }
        }

        if (newField.function().isPresent() || oldField.function().isPresent()) {
          var newDesc =
              newField.function().map(FlinkExecFunction::getFunctionDescription).orElse(null);
          var oldDesc =
              oldField.function().map(FlinkExecFunction::getFunctionDescription).orElse(null);
          if (!Objects.equals(newDesc, oldDesc)) {
            errors.warn(
                "Table '%s' field '%s' function changed from '%s' to '%s'",
                table.canonicalName(), newField.field().getName(), oldDesc, newDesc);
            compatible = false;
          }
        }
      }
    }

    return compatible;
  }

  public record Table(
      String canonicalName,
      String engine,
      String createTableSql,
      TableDefinition definition,
      Map<String, String> configOptions,
      String documentation) {}

  public record TableDefinition(
      List<ColumnDefinition> columns, List<String> primaryKey, List<String> partitionKey) {}

  public record ColumnDefinition(String name, String spec) {}
}
