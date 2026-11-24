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
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class SqlConvertersIntervalLiteralTest {

  private static final List<Dialect> DIALECTS =
      List.of(
          Dialect.POSTGRES,
          Dialect.DUCKDB,
          Dialect.TRINO,
          Dialect.SPARK_SQL,
          Dialect.SNOWFLAKE,
          Dialect.REDSHIFT);

  @ParameterizedTest
  @CsvSource({
    "WEEK, 2, INTERVAL '14' DAY",
    "QUARTER, 1, INTERVAL '3' MONTH",
    "DECADE, 1, INTERVAL '10' YEAR",
    "CENTURY, 1, INTERVAL '100' YEAR",
    "MILLENNIUM, 1, INTERVAL '1000' YEAR",
    "MILLISECOND, 5, INTERVAL '0.005' SECOND",
  })
  void givenIntervalCountedInAnotherUnit_whenConvertToSql_thenPrintsUnitOfValue(
      TimeUnit unit, int count, String expected) {
    for (var dialect : DIALECTS) {
      assertThat(convert(dialect, addInterval(unit, count)))
          .as(dialect.name())
          .contains(" + " + expected + ")");
    }
  }

  @ParameterizedTest
  @CsvSource({
    "YEAR, 2, INTERVAL '2' YEAR",
    "MONTH, 2, INTERVAL '2' MONTH",
    "DAY, 2, INTERVAL '2' DAY",
    "HOUR, 2, INTERVAL '2' HOUR",
    "MINUTE, 2, INTERVAL '2' MINUTE",
    "SECOND, 2, INTERVAL '2' SECOND",
  })
  void givenIntervalCountedInItsOwnUnit_whenConvertToSql_thenKeepsUnit(
      TimeUnit unit, int count, String expected) {
    for (var dialect : DIALECTS) {
      assertThat(convert(dialect, addInterval(unit, count)))
          .as(dialect.name())
          .contains(" + " + expected + ")");
    }
  }

  /**
   * Builds {@code SELECT ts + INTERVAL '<count>' <unit> FROM Records} with the literal that Flink
   * plans for it: the value in milliseconds or months, the qualifier as written.
   */
  private static RelNode addInterval(TimeUnit unit, int count) {
    var typeFactory = new JavaTypeFactoryImpl();
    var rexBuilder = new RexBuilder(typeFactory);
    var cluster = RelOptCluster.create(new VolcanoPlanner(), rexBuilder);
    var rowType = typeFactory.builder().add("ts", SqlTypeName.TIMESTAMP).build();
    var table = RelOptTableImpl.create(null, rowType, List.of("Records"), (Expression) null);
    var scan = LogicalTableScan.create(cluster, table, List.of());
    var interval =
        rexBuilder.makeIntervalLiteral(
            unit.multiplier.multiply(BigDecimal.valueOf(count)),
            new SqlIntervalQualifier(unit, null, SqlParserPos.ZERO));
    return LogicalProject.create(
        scan,
        List.of(),
        List.of(
            rexBuilder.makeCall(
                SqlStdOperatorTable.DATETIME_PLUS, rexBuilder.makeInputRef(scan, 0), interval)),
        List.of("shifted"));
  }

  private static String convert(Dialect dialect, RelNode relNode) {
    var converters = SqlConvertersFactory.get(dialect);
    return converters.convert(converters.convert(relNode, Map.of()));
  }
}
