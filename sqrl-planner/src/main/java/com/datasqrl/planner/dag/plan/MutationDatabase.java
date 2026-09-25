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
import com.datasqrl.deployment.model.MutationDatabaseModel;
import com.datasqrl.deployment.model.MutationDatabaseModel.ColumnDefinition;
import com.datasqrl.deployment.model.MutationDatabaseModel.Table;
import com.datasqrl.deployment.model.MutationDatabaseModel.TableDefinition;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.planner.RelDataTypeParser.ParsedRelDataTypeResult;
import com.datasqrl.planner.Sqrl2FlinkSQLTranslator;
import com.datasqrl.server.exec.FlinkExecFunction;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.flink.table.catalog.UniqueConstraint;

/** Builds and compares {@link MutationDatabaseModel}s during planning. */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class MutationDatabase {

  public static MutationDatabaseModel from(
      Collection<MutationTable> mutationTables, Sqrl2FlinkSQLTranslator env) {
    var tables =
        mutationTables.stream()
            .map(
                mutTbl -> {
                  var tblBuilder = mutTbl.getTableBuilder();
                  var createTableSql = tblBuilder.buildSql(false).toString();
                  var resolvedTable =
                      env.getRelDataTypeParser().parseToResolvedTable(createTableSql);
                  var columns =
                      resolvedTable.getResolvedSchema().getColumns().stream()
                          .map(
                              node -> {
                                var name = node.getName();
                                var entireColumn = node.asSummaryString();
                                var spec = entireColumn.substring(entireColumn.indexOf(' ') + 1);
                                var docs = mutTbl.getDocumentation().getColumn(name, null);
                                return new ColumnDefinition(name, spec, docs);
                              })
                          .toList();
                  var definition =
                      new TableDefinition(
                          columns,
                          resolvedTable
                              .getResolvedSchema()
                              .getPrimaryKey()
                              .map(UniqueConstraint::getColumns)
                              .orElse(List.of()),
                          resolvedTable.getPartitionKeys());
                  return new Table(
                      mutTbl.getName().getCanonical(),
                      mutTbl.getStage().name(),
                      createTableSql,
                      definition,
                      mutTbl.getCreateTable().getConfig(),
                      mutTbl.getDocumentation().getDocString(null));
                })
            .toList();

    return new MutationDatabaseModel(tables);
  }

  public static boolean isBackwardsCompatible(
      MutationDatabaseModel database,
      MutationDatabaseModel compareDb,
      Sqrl2FlinkSQLTranslator env,
      ErrorCollector errors) {
    var compareTablesByName =
        compareDb.tables().stream().collect(Collectors.toMap(Table::canonicalName, t -> t));

    var compatible = true;
    for (var table : database.tables()) {
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

      if (!hasUsableSchema(compareTable)) {
        errors.warn(
            "Table '%s' has no usable schema in the provided mutation database. Skipping schema compatibility check.",
            table.canonicalName());
        continue;
      }

      var parser = env.getRelDataTypeParser();
      var newSchema = parser.parseToRelDataType(table.createTableSql());
      List<ParsedRelDataTypeResult> oldSchema;
      try {
        oldSchema = parser.parseToRelDataType(compareTable.createTableSql());
      } catch (Exception e) {
        errors.warn(
            "Table '%s' has an unreadable schema in the provided mutation database. Skipping schema compatibility check: %s",
            table.canonicalName(), e.getMessage());
        continue;
      }

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

  private static boolean hasUsableSchema(Table table) {
    return table.definition() != null
        && table.definition().columns() != null
        && table.definition().columns().stream()
            .anyMatch(column -> column.name() != null && !column.name().isBlank());
  }
}
