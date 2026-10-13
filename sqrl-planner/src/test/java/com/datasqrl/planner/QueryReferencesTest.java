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
import static org.assertj.core.api.Assertions.tuple;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datasqrl.config.PackageJson.CompilerConfig;
import com.datasqrl.config.WorkspacePaths;
import com.datasqrl.engine.stream.flink.FlinkStreamEngine;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.loaders.schema.SchemaLoader;
import com.datasqrl.planner.QueryReferences.QueryReference;
import com.datasqrl.planner.hint.HintsAndDoc;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class QueryReferencesTest {

  @TempDir Path workspace;

  private Sqrl2FlinkSQLTranslator translator;
  private final List<QueryReference> references = new ArrayList<>();

  @BeforeEach
  void setUp() {
    var paths = new WorkspacePaths(workspace, workspace, workspace, workspace);
    var flink = mock(FlinkStreamEngine.class);
    when(flink.getExecutionMode()).thenReturn(RuntimeExecutionMode.STREAMING);
    when(flink.getBaseConfiguration()).thenReturn(new Configuration());
    when(flink.getStreamingSpecificConfig()).thenReturn(new Configuration());
    var config = mock(CompilerConfig.class);
    when(config.predicatePushdownRules()).thenReturn(PredicatePushdownRules.DEFAULT);
    translator = new Sqrl2FlinkSQLTranslator(paths, flink, config);
    translator.createTable(
        "CREATE TABLE Orders (id INT, amount DOUBLE, customer ROW<name STRING>)"
            + " WITH ('connector'='datagen')",
        Optional.empty(),
        mock(SchemaLoader.class),
        HintsAndDoc.EMPTY);
    translator.setListener(
        new PlannerListener() {
          @Override
          public void queryValidated(SqlNode query, SqlValidator validator) {
            references.addAll(QueryReferences.collect(query, validator));
          }
        });
  }

  @AfterEach
  void tearDown() {
    translator.close();
  }

  @Test
  void givenViewQuery_whenValidated_thenResolvesTableAndColumnsWithPositions() {
    translator.addView(
        "CREATE VIEW Big AS SELECT o.id, amount, o.customer.name FROM Orders o WHERE o.amount > 10",
        HintsAndDoc.EMPTY,
        ErrorCollector.root());

    assertThat(references)
        .extracting(r -> r.position().getColumnNum(), r -> r.table().get(2), QueryReference::column)
        .containsExactlyInAnyOrder(
            tuple(62, "Orders", List.of()),
            tuple(27, "Orders", List.of("id")),
            tuple(33, "Orders", List.of("amount")),
            tuple(41, "Orders", List.of("customer", "name")),
            tuple(77, "Orders", List.of("amount")));
    assertThat(references)
        .filteredOn(r -> r.column().equals(List.of("amount")))
        .allSatisfy(
            r -> assertThat(r.type()).hasValueSatisfying(t -> assertThat(t).hasToString("DOUBLE")));
  }

  @Test
  void givenNestedFieldWithoutTable_whenValidated_thenResolvesTheColumnAndField() {
    translator.addView(
        "CREATE VIEW N AS SELECT customer.name AS n FROM Orders WHERE customer.name <> ''",
        HintsAndDoc.EMPTY,
        ErrorCollector.root());

    assertThat(references)
        .filteredOn(QueryReference::isColumn)
        .extracting(r -> r.position().getColumnNum(), QueryReference::column)
        .containsExactlyInAnyOrder(
            tuple(25, List.of("customer", "name")), tuple(62, List.of("customer", "name")));
    assertThat(references)
        .filteredOn(QueryReference::isColumn)
        .allSatisfy(
            r ->
                assertThat(r.type())
                    .hasValueSatisfying(t -> assertThat(t).hasToString("VARCHAR(2147483647)")));
  }

  @Test
  void givenOrderByColumnOfTheSelectList_whenValidated_thenResolvesItAsAColumnOfTheTable() {
    var sql = "CREATE VIEW S AS SELECT *, amount * 2 AS twice FROM Orders ORDER BY amount, twice";
    translator.addView(sql, HintsAndDoc.EMPTY, ErrorCollector.root());

    // An alias of an expression, e.g. `twice`, is no column of a table
    assertThat(references)
        .filteredOn(r -> r.position().getColumnNum() > sql.indexOf("ORDER BY"))
        .extracting(r -> r.position().getColumnNum(), QueryReference::column)
        .containsExactly(tuple(sql.indexOf("amount, twice") + 1, List.of("amount")));
  }

  @Test
  void givenWindow_whenValidated_thenResolvesPartitionAndOrderColumns() {
    translator.addView(
        "CREATE VIEW W AS SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY amount DESC) AS rn"
            + " FROM Orders",
        HintsAndDoc.EMPTY,
        ErrorCollector.root());

    assertThat(references)
        .filteredOn(r -> r.position().getColumnNum() > 25)
        .extracting(r -> r.position().getColumnNum(), QueryReference::column)
        .containsExactlyInAnyOrder(
            tuple(60, List.of("id")), tuple(72, List.of("amount")), tuple(96, List.of()));
  }

  @Test
  void givenColumnQualifiedWithTableName_whenValidated_thenResolvesTheQualifierAsTheTable() {
    translator.addView(
        "CREATE VIEW Q AS SELECT Orders.id FROM Orders WHERE Orders.amount > 1",
        HintsAndDoc.EMPTY,
        ErrorCollector.root());

    assertThat(references)
        .extracting(
            r -> r.position().getColumnNum(),
            r -> r.position().getEndColumnNum(),
            QueryReference::column)
        .containsExactlyInAnyOrder(
            tuple(25, 30, List.of()),
            tuple(25, 33, List.of("id")),
            tuple(40, 45, List.of()),
            tuple(53, 58, List.of()),
            tuple(53, 65, List.of("amount")));
  }

  @Test
  void givenAliasNamedLikeTheTable_whenValidated_thenDoesNotResolveTheAliasAsTheTable() {
    translator.addView(
        "CREATE VIEW Q AS SELECT Orders.id FROM Orders AS Orders",
        HintsAndDoc.EMPTY,
        ErrorCollector.root());

    assertThat(references)
        .extracting(r -> r.position().getColumnNum(), QueryReference::column)
        .containsExactlyInAnyOrder(tuple(25, List.of("id")), tuple(40, List.of()));
  }

  @Test
  void givenWindowTableFunction_whenValidated_thenResolvesItsTable() {
    translator.createTable(
        "CREATE TABLE Events (id INT, ts TIMESTAMP(3), WATERMARK FOR ts AS ts)"
            + " WITH ('connector'='datagen')",
        Optional.empty(),
        mock(SchemaLoader.class),
        HintsAndDoc.EMPTY);

    translator.addView(
        "CREATE VIEW W AS SELECT window_start, COUNT(id) AS n FROM TABLE("
            + "TUMBLE(TABLE Events, DESCRIPTOR(ts), INTERVAL '1' MINUTE))"
            + " GROUP BY window_start, window_end",
        HintsAndDoc.EMPTY,
        ErrorCollector.root());

    assertThat(references)
        .filteredOn(r -> !r.isColumn())
        .extracting(r -> r.position().getColumnNum(), r -> r.table().get(2))
        .containsExactly(tuple(78, "Events"));
  }

  @Test
  void givenColumnOfSubquery_whenValidated_thenOnlyResolvesTheNamesInTheSubquery() {
    translator.addView(
        "CREATE VIEW Ids AS SELECT x.id FROM (SELECT id FROM Orders) x",
        HintsAndDoc.EMPTY,
        ErrorCollector.root());

    assertThat(references)
        .extracting(r -> r.position().getColumnNum(), QueryReference::column)
        .containsExactlyInAnyOrder(tuple(53, List.of()), tuple(45, List.of("id")));
  }
}
