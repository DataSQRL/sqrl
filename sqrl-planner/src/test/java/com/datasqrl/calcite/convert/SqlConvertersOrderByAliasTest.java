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
package com.datasqrl.calcite.convert;

import static org.assertj.core.api.Assertions.assertThat;

import com.datasqrl.calcite.Dialect;
import java.util.List;
import java.util.Map;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.calcite.rel.RelFieldCollation.NullDirection;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class SqlConvertersOrderByAliasTest {

  @Test
  void givenMultipartTableIdentifier_whenConvertToRedshiftSql_thenQualifiesEachIdentifierPart() {
    var converters = SqlConvertersFactory.get(Dialect.REDSHIFT);
    var sql =
        converters.convert(
            converters.convert(
                sortBySourceIdDesc("rid"),
                Map.of(
                    "Records",
                    new SqlIdentifier(
                        List.of("awsdatacatalog", "sqrl", "deployment_orders"),
                        SqlParserPos.ZERO))));

    assertThat(sql).contains("FROM \"awsdatacatalog\".\"sqrl\".\"deployment_orders\"");
  }

  @ParameterizedTest
  @EnumSource(
      value = Dialect.class,
      names = {"POSTGRES", "DUCKDB", "SNOWFLAKE", "SPARK_SQL", "REDSHIFT", "TRINO"})
  void givenOutputAliasShadowingSortColumn_whenConvertToSql_thenOrdersByOrdinal(Dialect dialect) {
    var sql = convert(dialect, sortBySourceIdDesc("source_id"));

    assertThat(sql).contains("ORDER BY 2 DESC").doesNotContainPattern("ORDER BY [\"`]source_id");
  }

  @ParameterizedTest
  @EnumSource(
      value = Dialect.class,
      names = {"POSTGRES", "DUCKDB", "SNOWFLAKE", "SPARK_SQL", "REDSHIFT", "TRINO"})
  void givenNonConflictingOutputAlias_whenConvertToSql_thenOrdersByInputColumn(Dialect dialect) {
    var sql = convert(dialect, sortBySourceIdDesc("rid"));

    assertThat(sql)
        .containsPattern("ORDER BY [\"`]source_id[\"`] DESC")
        .doesNotContain("ORDER BY 2");
  }

  /**
   * Builds {@code SELECT UPPER(patient_id) AS <firstAlias>, source_id AS original_source_id FROM
   * Records ORDER BY Records.source_id DESC}.
   */
  private static RelNode sortBySourceIdDesc(String firstAlias) {
    var typeFactory = new JavaTypeFactoryImpl();
    var rexBuilder = new RexBuilder(typeFactory);
    var cluster = RelOptCluster.create(new VolcanoPlanner(), rexBuilder);
    var rowType =
        typeFactory
            .builder()
            .add("source_id", SqlTypeName.VARCHAR)
            .add("patient_id", SqlTypeName.VARCHAR)
            .build();
    var table = RelOptTableImpl.create(null, rowType, List.of("Records"), (Expression) null);
    var scan = LogicalTableScan.create(cluster, table, List.of());
    var project =
        LogicalProject.create(
            scan,
            List.of(),
            List.of(
                rexBuilder.makeCall(SqlStdOperatorTable.UPPER, rexBuilder.makeInputRef(scan, 1)),
                rexBuilder.makeInputRef(scan, 0)),
            List.of(firstAlias, "original_source_id"));
    return LogicalSort.create(
        project,
        RelCollations.of(new RelFieldCollation(1, Direction.DESCENDING, NullDirection.LAST)),
        null,
        null);
  }

  private static String convert(Dialect dialect, RelNode relNode) {
    var converters = SqlConvertersFactory.get(dialect);
    return converters.convert(converters.convert(relNode, Map.of()));
  }
}
