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
package com.datasqrl.planner;

import static org.assertj.core.api.Assertions.assertThat;

import com.datasqrl.engine.stream.flink.FlinkSqlNodes;
import com.datasqrl.engine.stream.flink.sql.RelToFlinkSql;
import com.datasqrl.io.tables.TableType;
import com.datasqrl.planner.analyzer.TableAnalysis;
import java.util.Optional;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.InsertConflictStrategy.ConflictBehavior;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.junit.jupiter.api.Test;

class FlinkInsertConflictPlannerTest {

  @Test
  void givenUpsertKeyValidationFailure_whenResolveConflict_thenAddsDeduplicateBehavior() {
    var targetTableId = ObjectIdentifier.of("default_catalog", "default_database", "target_table");
    var planBuilder = new FlinkPhysicalPlan.Builder(new Configuration());
    var planner = new FlinkInsertConflictPlanner(RuntimeExecutionMode.STREAMING, null, planBuilder);
    var table =
        TableAnalysis.builder().objectIdentifier(targetTableId).type(TableType.STATE).build();

    planner.addInsert(createSelect(), targetTableId, table, true);

    var validationFailure = upsertKeyValidationFailure(targetTableId);

    assertThat(planner.resolveUpsertKeyConflict(validationFailure)).isTrue();
    assertThat(RelToFlinkSql.convertToSqlString(planBuilder.getExecuteStatements()).get(0))
        .contains("ON CONFLICT DO DEDUPLICATE");
    assertThat(planner.resolveUpsertKeyConflict(validationFailure)).isFalse();
  }

  @Test
  void givenExplicitConflictBehavior_whenResolveConflict_thenPreservesExplicitBehavior() {
    var targetTableId = ObjectIdentifier.of("default_catalog", "default_database", "target_table");
    var planBuilder = new FlinkPhysicalPlan.Builder(new Configuration());
    var planner = new FlinkInsertConflictPlanner(RuntimeExecutionMode.STREAMING, null, planBuilder);
    var table =
        TableAnalysis.builder()
            .objectIdentifier(targetTableId)
            .type(TableType.STATE)
            .insertConflictBehavior(Optional.of(ConflictBehavior.ERROR))
            .build();

    planner.addInsert(createSelect(), targetTableId, table, true);

    assertThat(planner.resolveUpsertKeyConflict(upsertKeyValidationFailure(targetTableId)))
        .isTrue();
    assertThat(RelToFlinkSql.convertToSqlString(planBuilder.getExecuteStatements()).get(0))
        .contains("ON CONFLICT DO ERROR");
  }

  private static RuntimeException upsertKeyValidationFailure(ObjectIdentifier targetTableId) {
    return new RuntimeException(
        "The query has an upsert key that differs from the primary key for table '"
            + targetTableId.asSummaryString()
            + "'");
  }

  private static SqlSelect createSelect() {
    var selectList = new SqlNodeList(SqlParserPos.ZERO);
    selectList.add(new SqlIdentifier("*", SqlParserPos.ZERO));
    return new SqlSelect(
        SqlParserPos.ZERO,
        null,
        selectList,
        FlinkSqlNodes.identifier("source_table"),
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null);
  }
}
