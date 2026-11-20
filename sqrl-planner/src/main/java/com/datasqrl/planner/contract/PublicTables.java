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
package com.datasqrl.planner.contract;

import com.datasqrl.deployment.model.JdbcStatementModel.Type;
import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.engine.PhysicalPlan;
import com.datasqrl.engine.PhysicalPlan.PhysicalStagePlan;
import com.datasqrl.engine.database.AnalyticDatabaseEngine;
import com.datasqrl.engine.database.CombinedEnginePlan;
import com.datasqrl.engine.database.relational.CreateTableJdbcStatement;
import com.datasqrl.engine.database.relational.JdbcEngineCreateTable.Role;
import com.datasqrl.engine.database.relational.JdbcPhysicalPlan;
import com.datasqrl.engine.database.relational.JdbcStatement;
import com.datasqrl.planner.analyzer.TableAnalysis;
import com.datasqrl.planner.dag.nodes.TableNode;
import com.datasqrl.planner.tables.StorageType;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * Finds the public tables of a pipeline: the tables and views that are visible, are not test
 * tables, and land in a table-format engine such as Iceberg, where other systems read them. A table
 * that only lands in a transactional database or in the log is not one of them, since it serves the
 * API of the deployment.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
final class PublicTables {

  /**
   * @param deployedTables the tables that the deployed pipeline computes
   * @param plan the physical plan, which has the names of the tables and views in the engines
   * @return the public tables, as the engines store them
   */
  static List<ContractSubject> of(Collection<TableNode> deployedTables, PhysicalPlan plan) {
    var publicTables =
        deployedTables.stream().filter(node -> isPublic(node.getTableAnalysis())).toList();
    return plan.getStagePlans().stream()
        .filter(stagePlan -> stagePlan.stage().engine() instanceof AnalyticDatabaseEngine)
        .flatMap(
            stagePlan ->
                Stream.concat(
                    materializedTables(stagePlan, publicTables), views(stagePlan, publicTables)))
        .toList();
  }

  private static boolean isPublic(TableAnalysis table) {
    return !table.isSourceOrSink()
        && !table.getIdentifier().isHidden()
        && !table.getHints().isTest()
        && !table.getHints().isWorkload();
  }

  /** The tables that the pipeline writes into the engine, with the columns that it stores. */
  private static Stream<ContractSubject> materializedTables(
      PhysicalStagePlan stagePlan, List<TableNode> publicTables) {
    return statements(stagePlan.plan(), Type.TABLE)
        .filter(CreateTableJdbcStatement.class::isInstance)
        .map(CreateTableJdbcStatement.class::cast)
        .filter(
            statement ->
                statement.getEngineTable() != null
                    && statement.getEngineTable().role() == Role.MATERIALIZATION)
        .filter(
            statement ->
                publicTables.stream()
                    .map(TableNode::getTableAnalysis)
                    .anyMatch(table -> table == statement.getEngineTable().tableAnalysis()))
        // The plans of the query engines list the tables of the engine again: its own plan,
        // which comes first, says how it stores them
        .collect(
            Collectors.toMap(
                CreateTableJdbcStatement::getEngineTable,
                Function.identity(),
                (ofEngine, ofQueryEngine) -> ofEngine,
                LinkedHashMap::new))
        .values()
        .stream()
        .map(PublicTables::materializedTable);
  }

  private static ContractSubject materializedTable(CreateTableJdbcStatement statement) {
    var engineTable = statement.getEngineTable();
    return ContractSubject.ofComputedTable(
            engineTable.logicalName(), engineTable.datatype(), engineTable.tableAnalysis())
        .primaryKey(Objects.requireNonNullElse(statement.getPrimaryKey(), List.of()))
        .partitionKeys(Objects.requireNonNullElse(statement.getPartitionKey(), List.of()))
        .storageType(Optional.of(StorageType.TABLE))
        .storageName(Optional.of(statement.getName()))
        .build();
  }

  /**
   * The tables that the engine computes when they are queried, for which it has a view of the name
   * of the table.
   */
  private static Stream<ContractSubject> views(
      PhysicalStagePlan stagePlan, List<TableNode> publicTables) {
    var viewNames = statements(stagePlan.plan(), Type.VIEW).map(JdbcStatement::getName).toList();
    return publicTables.stream()
        .filter(node -> node.getChosenStage().equals(stagePlan.stage()))
        .map(TableNode::getTableAnalysis)
        .filter(table -> viewNames.contains(table.getObjectIdentifier().getObjectName()))
        .map(
            table ->
                ContractSubject.ofComputedTable(
                        table.getObjectIdentifier().getObjectName(), table.getRowType(), table)
                    .storageType(Optional.of(StorageType.VIEW))
                    .storageName(Optional.of(table.getObjectIdentifier().getObjectName()))
                    .build());
  }

  /** The statements of the plan of an engine, and of the plans of its query engines. */
  private static Stream<JdbcStatement> statements(EnginePhysicalPlan plan, Type type) {
    if (plan instanceof JdbcPhysicalPlan jdbcPlan) {
      return jdbcPlan.getStatementsForType(type).stream();
    }
    if (plan instanceof CombinedEnginePlan combined) {
      return combined.getPlans().values().stream().flatMap(part -> statements(part, type));
    }
    return Stream.empty();
  }
}
