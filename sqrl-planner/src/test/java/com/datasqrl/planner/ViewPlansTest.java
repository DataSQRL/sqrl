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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datasqrl.config.PackageJson.CompilerConfig;
import com.datasqrl.config.WorkspacePaths;
import com.datasqrl.engine.stream.flink.FlinkStreamEngine;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.loaders.schema.SchemaLoader;
import com.datasqrl.planner.hint.HintsAndDoc;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.catalog.CatalogView;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.QueryOperationCatalogView;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ViewPlansTest {

  private static final String ORDERS =
      """
      CREATE TABLE Orders (
        id INT,
        customerid INT,
        amount DOUBLE,
        ts TIMESTAMP_LTZ(3),
        WATERMARK FOR ts AS ts - INTERVAL '1' SECOND
      ) WITH ('connector'='datagen')\
      """;

  private static final String LARGE = "SELECT * FROM Orders WHERE amount > 10";

  /** Views that use views: with a top-level sort, a correlated subquery and a view used twice */
  private static final List<String> VIEWS =
      List.of(
          "CREATE VIEW Recent AS SELECT id, customerid, amount, ts FROM Orders ORDER BY ts DESC",
          "CREATE VIEW BigOrders AS SELECT * FROM Recent WHERE amount > 10",
          """
          CREATE VIEW Repeated AS SELECT l.*, EXISTS(SELECT 1 FROM BigOrders o
            WHERE o.customerid = l.customerid AND o.id <> l.id) AS repeated FROM BigOrders l\
          """,
          """
          CREATE VIEW Combined AS SELECT r.id, r.ts, r.repeated, l.amount
            FROM Repeated r JOIN BigOrders l ON r.id = l.id\
          """);

  @TempDir Path workspace;

  @Test
  void givenView_whenAdded_thenQueriesUseItsPlanWithItsSchema() {
    try (var translator = createTranslator()) {
      translator.addView(VIEWS.get(0), HintsAndDoc.EMPTY, ErrorCollector.root());

      var view = translator.getCatalogManager().getTableOrError(identifier("Recent"));
      var definition = catalogView(translator, "Recent");
      assertThat(isTemporary(translator, "Recent")).isTrue();
      assertThat(view.getResolvedTable().getOrigin()).isInstanceOf(QueryOperationCatalogView.class);
      assertThat(view.getResolvedSchema())
          .isEqualTo(
              translator.getCatalogManager().resolveCatalogView(definition).getResolvedSchema());
      assertThat(
              ((QueryOperationCatalogView) view.getResolvedTable().getOrigin()).getExpandedQuery())
          .isEqualTo(definition.getExpandedQuery());
    }
  }

  @Test
  void givenViewsOfViews_whenPlanned_thenTheirPlansAreThoseOfTheExpandedViews() {
    try (var reusing = createTranslator();
        var expanding = createTranslator()) {
      for (var view : VIEWS) {
        reusing.addView(view, HintsAndDoc.EMPTY, ErrorCollector.root());
        // The views as the planner adds them to Flink, without their top-level sort
        expanding.executeSQL(view.replace(" ORDER BY ts DESC", ""));
      }
      var query = "CREATE VIEW RepeatedOrders AS SELECT * FROM Combined WHERE repeated";

      assertThat(withNumberedCorrelations(plan(reusing, query)))
          .isEqualTo(withNumberedCorrelations(plan(expanding, query)));
    }
  }

  @Test
  void givenAlteredView_whenAddedAgain_thenQueriesUseItsNewPlan() {
    try (var translator = createTranslator()) {
      translator.addView(
          "CREATE VIEW Amounts AS SELECT id FROM Orders", HintsAndDoc.EMPTY, ErrorCollector.root());
      translator.addView(
          "ALTER VIEW Amounts AS SELECT id, amount FROM Orders",
          HintsAndDoc.EMPTY,
          ErrorCollector.root());

      var plan = plan(translator, "CREATE VIEW BigOrders AS SELECT amount FROM Amounts");

      assertThat(plan.getRowType().getFieldNames()).containsExactly("amount");
      assertThat(isTemporary(translator, "Amounts")).isTrue();
    }
  }

  @Test
  void givenTemporaryView_whenAdded_thenQueriesUseThatView() {
    try (var translator = createTranslator()) {
      translator.addView(
          "CREATE TEMPORARY VIEW Amounts AS SELECT id, amount FROM Orders",
          HintsAndDoc.EMPTY,
          ErrorCollector.root());

      var plan = plan(translator, "CREATE VIEW Totals AS SELECT SUM(amount) AS total FROM Amounts");

      assertThat(plan.getRowType().getFieldNames()).containsExactly("total");
      assertThat(
              translator
                  .getCatalogManager()
                  .getTableOrError(identifier("Amounts"))
                  .getResolvedTable()
                  .getOrigin())
          .isNotInstanceOf(QueryOperationCatalogView.class);
    }
  }

  @Test
  void givenReusedPlan_whenViewsAreRestored_thenQueriesExpandTheViewsForGood() {
    try (var translator = createTranslator()) {
      var plans = new ViewPlans(translator.getCatalogManager());
      translator.executeSQL("CREATE VIEW BigOrders AS " + LARGE);
      var plan = plan(translator, "CREATE VIEW Probe AS " + LARGE);
      plans.reuse(identifier("BigOrders"), plan);
      assertThat(isTemporary(translator, "BigOrders")).isTrue();

      plans.restoreViews();
      plans.reuse(identifier("BigOrders"), plan);

      assertThat(isTemporary(translator, "BigOrders")).isFalse();
    }
  }

  private static RelNode plan(Sqrl2FlinkSQLTranslator translator, String view) {
    return translator
        .analyzeView(translator.parseSQL(view), false, HintsAndDoc.EMPTY, ErrorCollector.root())
        .originalRelnode();
  }

  /**
   * The plan as text, with correlation variables numbered in order of appearance, since their ids
   * count the conversions of the translator.
   */
  private static String withNumberedCorrelations(RelNode plan) {
    var numbers = new HashMap<String, Integer>();
    var matcher = Pattern.compile("\\$cor\\d+").matcher(RelOptUtil.toString(plan));
    var text = new StringBuilder();
    while (matcher.find()) {
      var number = numbers.computeIfAbsent(matcher.group(), id -> numbers.size());
      matcher.appendReplacement(text, "\\$cor" + number);
    }
    return matcher.appendTail(text).toString();
  }

  /** Whether queries use a temporary view of the plan of a view, instead of the view itself */
  private static boolean isTemporary(Sqrl2FlinkSQLTranslator translator, String name) {
    return translator.getCatalogManager().getTableOrError(identifier(name)).isTemporary();
  }

  /** A view as the catalog has it */
  private static CatalogView catalogView(Sqrl2FlinkSQLTranslator translator, String name) {
    try {
      return (CatalogView)
          translator
              .getCatalogManager()
              .getCatalog("default_catalog")
              .orElseThrow()
              .getTable(new ObjectPath("default_database", name));
    } catch (Exception e) {
      throw new AssertionError(e);
    }
  }

  private static ObjectIdentifier identifier(String name) {
    return ObjectIdentifier.of("default_catalog", "default_database", name);
  }

  private Sqrl2FlinkSQLTranslator createTranslator() {
    var paths = new WorkspacePaths(workspace, workspace, workspace, workspace);
    var flink = mock(FlinkStreamEngine.class);
    when(flink.getExecutionMode()).thenReturn(RuntimeExecutionMode.STREAMING);
    when(flink.getBaseConfiguration()).thenReturn(new Configuration());
    when(flink.getStreamingSpecificConfig()).thenReturn(new Configuration());
    var config = mock(CompilerConfig.class);
    when(config.predicatePushdownRules()).thenReturn(PredicatePushdownRules.DEFAULT);
    var translator = new Sqrl2FlinkSQLTranslator(paths, flink, config);
    translator.createTable(ORDERS, Optional.empty(), mock(SchemaLoader.class), HintsAndDoc.EMPTY);
    return translator;
  }
}
