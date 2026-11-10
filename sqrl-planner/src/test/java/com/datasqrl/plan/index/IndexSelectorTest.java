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
package com.datasqrl.plan.index;

import static com.datasqrl.engine.database.relational.IndexSelectorConfigByDialect.DEFAULT_COST_THRESHOLD;
import static com.datasqrl.plan.index.IndexSelector.getFallbackIndexColumns;
import static com.datasqrl.plan.index.IndexSelector.improvesAnyQueryByThreshold;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datasqrl.engine.database.relational.CreateTableJdbcStatement;
import com.datasqrl.engine.database.relational.JdbcEngineCreateTable;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.plan.index.IndexSelector.NamedTable;
import com.datasqrl.planner.Sqrl2FlinkSQLTranslator;
import com.datasqrl.planner.analyzer.TableAnalysis;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Holder;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class IndexSelectorTest {

  @Test
  void givenProjectedColumnSort_whenSummarize_thenIdentifiesColumn() {
    var fieldType = mock(RelDataType.class);
    var field = mock(RelDataTypeField.class);
    when(field.getType()).thenReturn(fieldType);
    var rowType = mock(RelDataType.class);
    when(rowType.getFieldList()).thenReturn(List.of(field));

    var summary =
        QueryIndexSummary.ofSort(
                new NamedTable("orders", "orders", null, null), RexInputRef.of(0, rowType))
            .orElseThrow();

    assertThat(summary.getInequalityColumns()).containsExactly(0);
  }

  @Test
  void givenSort_whenGenerateIndexCandidates_thenBtreeUsesDefaultSortOrder() {
    var rowType = mock(RelDataType.class);
    when(rowType.getFieldNames()).thenReturn(List.of("col_a"));
    var relNode = mock(RelNode.class);
    when(relNode.getRowType()).thenReturn(rowType);
    var tableAnalysis =
        TableAnalysis.builder()
            .objectIdentifier(ObjectIdentifier.of("datasqrl", "public", "orders"))
            .collapsedRelnode(relNode)
            .originalRelnode(relNode)
            .build();
    var table = new NamedTable("orders", "orders", tableAnalysis, null);
    var summary = new QueryIndexSummary(table, Set.of(), Set.of(0), Set.of(), 1.0);
    var config = mock(IndexSelectorConfig.class);
    when(config.supportedIndexTypes()).thenReturn(EnumSet.of(IndexType.BTREE));
    when(config.maxIndexColumns(IndexType.BTREE)).thenReturn(1);

    var candidates =
        new IndexSelector(null, config, Map.of(), ErrorCollector.root())
            .generateIndexCandidates(summary);

    assertThat(candidates)
        .singleElement()
        .satisfies(
            index -> {
              assertThat(index.getColumns()).containsExactly(0);
              assertThat(index.getDirections()).containsExactly(Direction.ASCENDING);
            });
  }

  private static final double FULL_SCAN = 100.0;
  private static final double INDEX_LOOKUP = 5.0;

  /** Issue #2317: unrelated queries on the table must not influence the decision. */
  @Test
  void givenIndexServingOneQuery_whenTableHasManyUnrelatedQueries_thenWorthIndexing() {
    for (var numQueries = 1; numQueries <= 100; numQueries++) {
      var before = fullScans(numQueries);
      var after = new HashMap<>(before);
      after.put(before.keySet().iterator().next(), INDEX_LOOKUP);

      assertThat(improvesAnyQueryByThreshold(before, after, DEFAULT_COST_THRESHOLD))
          .as("%d queries on the table", numQueries)
          .isTrue();
    }
  }

  @Test
  void givenIndexServingEveryQuery_whenEvaluated_thenWorthIndexing() {
    var before = fullScans(5);
    Map<QueryIndexSummary, Double> after = new LinkedHashMap<>();
    before.keySet().forEach(query -> after.put(query, INDEX_LOOKUP));

    assertThat(improvesAnyQueryByThreshold(before, after, DEFAULT_COST_THRESHOLD)).isTrue();
  }

  @Test
  void givenIndexThatBarelyImprovesTheQueryItServes_whenEvaluated_thenNotWorthIndexing() {
    var before = fullScans(5);
    var after = new HashMap<>(before);
    after.put(before.keySet().iterator().next(), FULL_SCAN * 0.99);

    assertThat(improvesAnyQueryByThreshold(before, after, DEFAULT_COST_THRESHOLD)).isFalse();
  }

  @Test
  void givenIndexThatImprovesNoQuery_whenEvaluated_thenNotWorthIndexing() {
    var before = fullScans(5);

    assertThat(improvesAnyQueryByThreshold(before, new HashMap<>(before), DEFAULT_COST_THRESHOLD))
        .isFalse();
  }

  /** Issue #2317: a synthetic {@code __pk_hash} key leaves column 0 needing an index of its own. */
  @Test
  void givenSyntheticHashPrimaryKey_whenFallbackToColumnIndexes_thenFirstColumnIsIndexed() {
    var hashKey = primaryKeyIndex(3, "__pk_hash");

    assertThat(getFallbackIndexColumns(queriesFilteringOn(0, 1, 2), hashKey))
        .containsExactly(0, 1, 2);
  }

  @Test
  void givenPrimaryKeyOnFirstColumn_whenFallbackToColumnIndexes_thenFirstColumnIsNotIndexed() {
    var naturalKey = primaryKeyIndex(0, "orderId");

    assertThat(getFallbackIndexColumns(queriesFilteringOn(0, 1, 2), naturalKey))
        .containsExactly(1, 2);
  }

  @Test
  void givenNoPrimaryKeyIndex_whenFallbackToColumnIndexes_thenEveryColumnIsIndexed() {
    assertThat(getFallbackIndexColumns(queriesFilteringOn(0, 1, 2), Optional.empty()))
        .containsExactly(0, 1, 2);
  }

  /**
   * Issue #2471: the query of a subquery is analyzed with the correlated columns as parameters, and
   * an {@code EXISTS} or {@code IN} conjunct also lets the outer table be looked up.
   */
  @Nested
  class SubQueries {

    private final FlinkTypeFactory typeFactory =
        new FlinkTypeFactory(getClass().getClassLoader(), FlinkTypeSystem.INSTANCE);
    private final RelDataType bigint = typeFactory.createSqlType(SqlTypeName.BIGINT);
    private final Map<String, CreateTableJdbcStatement> tableMap = new HashMap<>();
    private final RelBuilder builder = createRelBuilder();
    private final RexNode accountParam = builder.getRexBuilder().makeDynamicParam(bigint, 0);
    private final RexNode agencyParam = builder.getRexBuilder().makeDynamicParam(bigint, 1);

    @Test
    void givenExists_whenGetIndexSelection_thenEitherTableCanLookUpTheOther() {
      var orders = Holder.<RexCorrelVariable>empty();
      builder.scan("Orders").variable(orders);
      var query =
          builder
              .filter(
                  List.of(orders.get().id),
                  builder.exists(b -> ownersOfAccount(b, orders.get()).build()))
              .build();

      assertThat(describe(query))
          .containsExactlyInAnyOrder(
              "Orders eq[customer_id]",
              "Owners eq[account_id, customer_id]",
              "Owners eq[account_id]");
    }

    @Test
    void givenIn_whenGetIndexSelection_thenOuterColumnIsAnEqualityColumn() {
      var owners =
          builder
              .scan("Owners")
              .filter(builder.equals(builder.field("account_id"), accountParam))
              .project(builder.field("customer_id"))
              .build();
      builder.scan("Orders");
      var query = builder.filter(builder.in(owners, List.of(builder.field("customer_id")))).build();

      assertThat(describe(query))
          .containsExactlyInAnyOrder(
              "Orders eq[customer_id]",
              "Owners eq[account_id, customer_id]",
              "Owners eq[account_id]");
    }

    @Test
    void givenNotExists_whenGetIndexSelection_thenOnlySubqueryTableIsLookedUp() {
      var orders = Holder.<RexCorrelVariable>empty();
      builder.scan("Orders").variable(orders);
      var query =
          builder
              .filter(
                  List.of(orders.get().id),
                  builder.not(builder.exists(b -> ownersOfAccount(b, orders.get()).build())))
              .build();

      assertThat(describe(query))
          .containsExactlyInAnyOrder("Owners eq[account_id, customer_id]", "Owners eq[account_id]");
    }

    @Test
    void givenNotIn_whenGetIndexSelection_thenSubqueryIsFilteredByItsOwnConditionOnly() {
      var owners =
          builder
              .scan("Owners")
              .filter(builder.equals(builder.field("account_id"), accountParam))
              .project(builder.field("customer_id"))
              .build();
      builder.scan("Orders");
      var query =
          builder
              .filter(builder.not(builder.in(owners, List.of(builder.field("customer_id")))))
              .build();

      assertThat(describe(query)).containsExactly("Owners eq[account_id]");
    }

    @Test
    void givenExistsInDisjunction_whenGetIndexSelection_thenOuterCorrelationIsNotIndexed() {
      var orders = Holder.<RexCorrelVariable>empty();
      builder.scan("Orders").variable(orders);
      var query =
          builder
              .filter(
                  List.of(orders.get().id),
                  builder.or(
                      builder.equals(builder.field("agency_id"), agencyParam),
                      builder.exists(b -> ownersOfAccount(b, orders.get()).build())))
              .build();

      assertThat(describe(query))
          .containsExactlyInAnyOrder(
              "Orders eq[agency_id]",
              "Owners eq[account_id, customer_id]",
              "Owners eq[account_id]");
    }

    @Test
    void givenCorrelatedScalarSubquery_whenGetIndexSelection_thenSubqueryIsLookedUpPerRow() {
      var orders = Holder.<RexCorrelVariable>empty();
      builder.scan("Orders").variable(orders);
      var count =
          builder.scalarQuery(
              b ->
                  ownersOfAccount(b, orders.get())
                      .aggregate(b.groupKey(), b.countStar("c"))
                      .build());
      var query =
          builder.filter(List.of(orders.get().id), builder.greaterThan(count, agencyParam)).build();

      assertThat(describe(query)).containsExactly("Owners eq[account_id, customer_id]");
    }

    @Test
    void givenUncorrelatedScalarSubquery_whenGetIndexSelection_thenItActsAsParameter() {
      builder.scan("Orders");
      var maxCustomer =
          builder.scalarQuery(
              b ->
                  b.scan("Owners")
                      .filter(b.equals(b.field("account_id"), accountParam))
                      .aggregate(b.groupKey(), b.max(b.field("customer_id")))
                      .build());
      var query = builder.filter(builder.equals(builder.field("customer_id"), maxCustomer)).build();

      assertThat(describe(query))
          .containsExactlyInAnyOrder("Orders eq[customer_id]", "Owners eq[account_id]");
    }

    /** The owners of the account parameter that are correlated with the customer of an order. */
    private RelBuilder ownersOfAccount(RelBuilder b, RexCorrelVariable orders) {
      return b.scan("Owners")
          .filter(
              b.equals(b.field("account_id"), accountParam),
              b.equals(b.field("customer_id"), b.field(orders, "customer_id")));
    }

    private List<String> describe(RelNode query) {
      var framework = mock(Sqrl2FlinkSQLTranslator.class);
      when(framework.getTypeFactory()).thenReturn(typeFactory);
      var selection =
          new IndexSelector(framework, null, tableMap, ErrorCollector.root())
              .getIndexSelection(query);
      return selection.summaries().stream()
          .map(
              summary -> {
                var names = summary.getTable().getAnalysis().getRowType().getFieldNames();
                return summary.getTable().getTableId()
                    + " eq"
                    + summary.getEqualityColumns().stream().sorted().map(names::get).toList()
                    + (summary.getInequalityColumns().isEmpty()
                        ? ""
                        : " iq"
                            + summary.getInequalityColumns().stream()
                                .sorted()
                                .map(names::get)
                                .toList());
              })
          .toList();
    }

    private RelBuilder createRelBuilder() {
      var rootSchema = CalciteSchema.createRootSchema(false);
      addTable(rootSchema, "Orders", List.of("order_id", "customer_id", "agency_id"));
      addTable(rootSchema, "Owners", List.of("account_id", "customer_id"));
      var catalogReader =
          new CalciteCatalogReader(
              rootSchema, List.of(), typeFactory, CalciteConnectionConfig.DEFAULT);
      var cluster =
          RelOptCluster.create(
              new HepPlanner(HepProgram.builder().build()), new RexBuilder(typeFactory));
      return RelFactories.LOGICAL_BUILDER.create(cluster, catalogReader);
    }

    private void addTable(CalciteSchema schema, String name, List<String> columns) {
      var rowTypeBuilder = typeFactory.builder();
      columns.forEach(column -> rowTypeBuilder.add(column, bigint));
      var rowType = rowTypeBuilder.build();
      schema.add(
          name,
          new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory factory) {
              return rowType;
            }
          });
      var relNode = mock(RelNode.class);
      when(relNode.getRowType()).thenReturn(rowType);
      var analysis =
          TableAnalysis.builder()
              .objectIdentifier(ObjectIdentifier.of("datasqrl", "public", name))
              .collapsedRelnode(relNode)
              .originalRelnode(relNode)
              .build();
      var stmt = mock(CreateTableJdbcStatement.class);
      when(stmt.getEngineTable())
          .thenReturn(new JdbcEngineCreateTable(name, name, null, null, rowType, analysis));
      tableMap.put(name, stmt);
    }
  }

  private static Optional<IndexDefinition> primaryKeyIndex(int column, String columnName) {
    return Optional.of(
        IndexDefinition.getPrimaryKeyIndex("Orders", List.of(column), List.of(columnName)));
  }

  private static List<QueryIndexSummary> queriesFilteringOn(int... columns) {
    return IntStream.of(columns)
        .mapToObj(column -> new QueryIndexSummary(null, Set.of(column), Set.of(), Set.of(), 1.0))
        .collect(Collectors.toList());
  }

  private static Map<QueryIndexSummary, Double> fullScans(int numQueries) {
    Map<QueryIndexSummary, Double> costs = new LinkedHashMap<>();
    for (var column = 0; column < numQueries; column++) {
      costs.put(new QueryIndexSummary(null, Set.of(column), Set.of(), Set.of(), 1.0), FULL_SCAN);
    }
    return costs;
  }
}
