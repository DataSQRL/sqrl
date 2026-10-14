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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datasqrl.engine.database.relational.JdbcEngineCreateTable.Role;
import java.util.List;
import java.util.Map;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptAbstractTable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.junit.jupiter.api.Test;

class AbstractJdbcStatementFactoryTest {

  private static final Map<String, JdbcEngineCreateTable> TABLE_ID_MAP =
      Map.of(
          "Orders_2", table("dev_orders_2"),
          "Spend_1", table("dev_spend_1"),
          "Customers_3", table("dev_customers_3"));

  @Test
  void givenJoinWithSubQuery_whenGettingReferencedTables_thenReturnsSortedTableNames() {
    var builder = relBuilder();
    var relNode =
        builder
            .push(scan(builder, "Orders_2"))
            .push(scan(builder, "Spend_1"))
            .join(
                JoinRelType.INNER,
                builder.equals(builder.field(2, 0, "id"), builder.field(2, 1, "id")))
            .filter(builder.exists(b -> scan(b, "Customers_3")))
            .build();

    assertThat(AbstractJdbcStatementFactory.getReferencedTables(relNode, TABLE_ID_MAP))
        .containsExactly("dev_customers_3", "dev_orders_2", "dev_spend_1");
  }

  @Test
  void givenSelfJoin_whenGettingReferencedTables_thenListsTableOnce() {
    var builder = relBuilder();
    var relNode =
        builder
            .push(scan(builder, "Orders_2"))
            .push(scan(builder, "Orders_2"))
            .join(
                JoinRelType.INNER,
                builder.equals(builder.field(2, 0, "id"), builder.field(2, 1, "id")))
            .build();

    assertThat(AbstractJdbcStatementFactory.getReferencedTables(relNode, TABLE_ID_MAP))
        .containsExactly("dev_orders_2");
  }

  @Test
  void givenScanOfUnknownTable_whenGettingReferencedTables_thenThrows() {
    var relNode = scan(relBuilder(), "Unknown_4");

    assertThatThrownBy(
            () -> AbstractJdbcStatementFactory.getReferencedTables(relNode, TABLE_ID_MAP))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Unknown_4");
  }

  private static JdbcEngineCreateTable table(String tableName) {
    return new JdbcEngineCreateTable(tableName, null, Role.MATERIALIZATION, null, null, null);
  }

  private static RelBuilder relBuilder() {
    var cluster =
        RelOptCluster.create(new VolcanoPlanner(), new RexBuilder(new JavaTypeFactoryImpl()));
    return RelBuilder.proto(Contexts.empty()).create(cluster, null);
  }

  private static RelNode scan(RelBuilder builder, String tableId) {
    var cluster = builder.getCluster();
    var rowType = cluster.getTypeFactory().builder().add("id", SqlTypeName.BIGINT).build();
    var table = new RelOptAbstractTable(null, tableId, rowType) {};
    return LogicalTableScan.create(cluster, table, List.of());
  }
}
