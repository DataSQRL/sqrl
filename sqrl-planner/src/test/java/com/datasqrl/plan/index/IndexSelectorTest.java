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

import static com.datasqrl.plan.index.IndexSelector.getFallbackIndexColumns;
import static com.datasqrl.plan.index.IndexSelector.improvesAnyQueryByThreshold;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datasqrl.config.JdbcDialect;
import com.datasqrl.config.PackageJson.IndexSelectionConfig;
import com.datasqrl.engine.database.relational.CreateTableJdbcStatement;
import com.datasqrl.engine.database.relational.IndexSelectorConfigByDialect;
import com.datasqrl.engine.database.relational.JdbcEngineCreateTable;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorMessage;
import com.datasqrl.plan.index.IndexSelector.NamedTable;
import com.datasqrl.plan.table.TableStatistic;
import com.datasqrl.plan.util.PrimaryKeyMap;
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
import java.util.stream.Stream;
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
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class IndexSelectorTest {

  /** The threshold of the default {@code min-cost-reduction} of 0.05. */
  private static final double DEFAULT_COST_THRESHOLD = 0.95;

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

    /** Postgres reads the outer table first when a join condition is not an equality. */
    @Test
    void givenExistsWithRangeCorrelation_whenGetIndexSelection_thenOnlySubqueryTableIsLookedUp() {
      var orders = Holder.<RexCorrelVariable>empty();
      builder.scan("Orders").variable(orders);
      var query =
          builder
              .filter(
                  List.of(orders.get().id),
                  builder.exists(
                      b ->
                          b.scan("Owners")
                              .filter(
                                  b.equals(b.field("account_id"), accountParam),
                                  b.greaterThan(
                                      b.field("customer_id"), b.field(orders.get(), "customer_id")))
                              .build()))
              .build();

      assertThat(describe(query))
          .containsExactlyInAnyOrder(
              "Owners eq[account_id] iq[customer_id]", "Owners eq[account_id]");
    }

    @Test
    void givenExistsWithEqualityAndRangeCorrelation_whenGetIndexSelection_thenOuterIsNotLookedUp() {
      var orders = Holder.<RexCorrelVariable>empty();
      builder.scan("Orders").variable(orders);
      var query =
          builder
              .filter(
                  List.of(orders.get().id),
                  builder.exists(
                      b ->
                          ownersOfAccount(b, orders.get())
                              .filter(b.greaterThan(b.field("ts"), b.field(orders.get(), "ts")))
                              .build()))
              .build();

      assertThat(describe(query))
          .containsExactlyInAnyOrder(
              "Owners eq[account_id, customer_id] iq[ts]", "Owners eq[account_id]");
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
      addTable(rootSchema, "Orders", List.of("order_id", "customer_id", "agency_id", "ts"));
      addTable(rootSchema, "Owners", List.of("account_id", "customer_id", "ts"));
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

  /** Issue #2622: the distinct count of a {@code row_count} hint decides what is worth indexing. */
  @Nested
  class DistinctCounts {

    @Test
    void givenNoDistinctCounts_whenOptimizeIndexes_thenIndexesBothEqualityColumns() {
      var customers = customersWith(TableStatistic.fromHint(1e6));

      assertThat(indexesForEqualityOn(customers, EMAIL, COUNTRY))
          .containsExactly("BTREE[email, country]");
    }

    @Test
    void givenUniqueColumn_whenOptimizeIndexes_thenIndexesOnlyThatColumn() {
      var customers =
          customersWith(
              TableStatistic.fromHint(1e6).withDistinctCounts(Map.of(Set.of(EMAIL), 1e6)));

      assertThat(indexesForEqualityOn(customers, EMAIL, COUNTRY)).containsExactly("HASH[email]");
    }

    @Test
    void givenUniqueColumnCombination_whenOptimizeIndexes_thenIndexesOnlyItsColumns() {
      var customers =
          customersWith(
              TableStatistic.fromHint(1e6).withDistinctCounts(Map.of(Set.of(COUNTRY, CITY), 1e6)));

      assertThat(indexesForEqualityOn(customers, EMAIL, COUNTRY, CITY))
          .containsExactly("BTREE[country, city]");
    }

    @Test
    void givenColumnThatAddsFewDistinctValues_whenOptimizeIndexes_thenLeavesItOutOfTheIndex() {
      var customers =
          customersWith(
              TableStatistic.fromHint(1e6)
                  .withDistinctCounts(Map.of(Set.of(COUNTRY), 200.0, Set.of(COUNTRY, CITY), 2e3)));

      assertThat(indexesForEqualityOn(customers, COUNTRY, CITY)).containsExactly("HASH[country]");
    }

    @Test
    void givenColumnWithFewDistinctValues_whenOptimizeIndexes_thenLeavesItOutOfTheIndex() {
      var customers =
          customersWith(
              TableStatistic.fromHint(1e6).withDistinctCounts(Map.of(Set.of(COUNTRY), 5.0)));

      assertThat(indexesForEqualityOn(customers, EMAIL, COUNTRY)).containsExactly("HASH[email]");
    }

    @Test
    void givenOnlyColumnWithFewDistinctValues_whenOptimizeIndexes_thenCreatesNoIndex() {
      var customers =
          customersWith(
              TableStatistic.fromHint(1e6).withDistinctCounts(Map.of(Set.of(COUNTRY), 5.0)));

      assertThat(indexesForEqualityOn(customers, COUNTRY)).isEmpty();
    }

    /** Both queries filter on the email column once the country column is left out. */
    @Test
    void givenQueriesThatDifferInColumnWithFewDistinctValues_whenOptimizeIndexes_thenShareIndex() {
      var customers =
          customersWith(
              TableStatistic.fromHint(1e6).withDistinctCounts(Map.of(Set.of(COUNTRY), 5.0)));
      var queries =
          List.of(
              new QueryIndexSummary(customers, Set.of(EMAIL, COUNTRY), Set.of(), Set.of(), 1.0),
              new QueryIndexSummary(customers, Set.of(EMAIL), Set.of(), Set.of(), 1.0));

      assertThat(selectIndexes(postgres(8, 50), queries)).containsExactly("HASH[email]");
    }

    /** The primary key index on the country and email columns serves the query as it is. */
    @Test
    void givenPrimaryKeyLookupWithColumnOfFewDistinctValues_whenOptimizeIndexes_thenNoIndex() {
      var customers =
          customersWith(
              TableStatistic.fromHint(1e6).withDistinctCounts(Map.of(Set.of(COUNTRY), 5.0)),
              List.of(COUNTRY, EMAIL));

      assertThat(indexesForEqualityOn(customers, COUNTRY, EMAIL)).isEmpty();
    }

    /** The primary key index serves the equality on country and the range on email. */
    @Test
    void givenPrimaryKeyRangeScanWithColumnOfFewDistinctValues_whenOptimizeIndexes_thenNoIndex() {
      var customers =
          customersWith(
              TableStatistic.fromHint(1e6).withDistinctCounts(Map.of(Set.of(COUNTRY), 5.0)),
              List.of(COUNTRY, EMAIL));
      var countryAndEmailRange =
          new QueryIndexSummary(customers, Set.of(COUNTRY), Set.of(EMAIL), Set.of(), 1.0);

      assertThat(selectIndexes(postgres(8, 50), List.of(countryAndEmailRange))).isEmpty();
    }

    private List<String> indexesForEqualityOn(NamedTable table, Integer... equalityColumns) {
      var query = new QueryIndexSummary(table, Set.of(equalityColumns), Set.of(), Set.of(), 1.0);

      return selectIndexes(postgres(8, 50), List.of(query));
    }
  }

  /** A table whose filter patterns need more indexes than {@code max-indexes}. */
  @Nested
  class MoreIndexesThanMaxIndexes {

    private static final int MAX_INDEXES = 2;

    private final NamedTable customers = customersWith(TableStatistic.fromHint(1e6));

    @Test
    void givenMoreCompositeIndexesThanColumns_whenOptimizeIndexes_thenIndexesEachColumn() {
      var queries =
          List.of(
              equalityAndRangeOn(EMAIL, COUNTRY),
              equalityAndRangeOn(EMAIL, CITY),
              equalityAndRangeOn(COUNTRY, EMAIL),
              equalityAndRangeOn(COUNTRY, CITY),
              equalityAndRangeOn(CITY, EMAIL),
              equalityAndRangeOn(CITY, COUNTRY));

      var errors = ErrorCollector.root();

      var indexes = selectIndexes(postgres(MAX_INDEXES, 50), queries, errors);

      assertThat(indexes).containsExactly("BTREE[email]", "BTREE[country]", "BTREE[city]");
      assertThat(errors)
          .extracting(ErrorMessage::getMessage)
          .singleElement(InstanceOfAssertFactories.STRING)
          .startsWith(
              "The query filter patterns of table `Customers` need 6 indexes which exceeds the "
                  + "maximum of 2. Created a single-column index for each filtered column "
                  + "instead, and none of the 6 indexes. The query results are not affected.");
    }

    /** The country column has too few distinct values for an index of its own. */
    @Test
    void givenColumnWithFewDistinctValues_whenIndexesEachColumn_thenLeavesThatColumnOut() {
      var customersWithFewCountries =
          customersWith(
              TableStatistic.fromHint(1e6).withDistinctCounts(Map.of(Set.of(COUNTRY), 5.0)));
      var queries =
          Stream.of(
                  List.of(EMAIL, CITY),
                  List.of(EMAIL, ZIP),
                  List.of(CITY, EMAIL),
                  List.of(CITY, ZIP),
                  List.of(ZIP, EMAIL),
                  List.of(ZIP, CITY))
              .map(
                  equalityAndRangeColumn ->
                      new QueryIndexSummary(
                          customersWithFewCountries,
                          Set.of(COUNTRY, equalityAndRangeColumn.get(0)),
                          Set.of(equalityAndRangeColumn.get(1)),
                          Set.of(),
                          1.0))
              .toList();

      assertThat(selectIndexes(postgres(MAX_INDEXES, 50), queries))
          .containsExactly("BTREE[email]", "BTREE[city]", "BTREE[zip]");
    }

    @Test
    void givenAsManyIndexesAsColumns_whenOptimizeIndexes_thenKeepsTheFirstMaxIndexes() {
      var queries = List.of(equalityOn(EMAIL), equalityOn(COUNTRY), equalityOn(CITY));

      assertThat(selectIndexes(postgres(MAX_INDEXES, 50), queries))
          .containsExactly("HASH[email]", "HASH[country]");
    }

    @Test
    void givenOmittedIndexWithoutFullTableScan_whenOptimizeIndexes_thenWarnsAboutNeededIndexes() {
      var queries =
          List.of(
              equalityAndRangeOn(EMAIL, COUNTRY),
              equalityAndRangeOn(COUNTRY, EMAIL),
              equalityAndRangeOn(EMAIL, CITY));
      var errors = ErrorCollector.root();

      var indexes = selectIndexes(postgres(MAX_INDEXES, 50), queries, errors);

      assertThat(indexes).hasSize(MAX_INDEXES);
      assertThat(errors)
          .extracting(ErrorMessage::getMessage)
          .singleElement(InstanceOfAssertFactories.STRING)
          .startsWith(
              "Created the maximum of 2 indexes for table `Customers`. Its query filter patterns "
                  + "need 3 indexes. The limit omits these indexes: index(BTREE, email, city). "
                  + "The query results are not affected.")
          .doesNotContain("full table scan");
    }

    @Test
    void givenIndexesWithinMaxIndexes_whenOptimizeIndexes_thenKeepsCompositeIndexes() {
      var queries = List.of(equalityAndRangeOn(EMAIL, COUNTRY), equalityAndRangeOn(COUNTRY, EMAIL));

      assertThat(selectIndexes(postgres(MAX_INDEXES, 50), queries))
          .containsExactly("BTREE[email, country]", "BTREE[country, email]");
    }

    /** A partitioned table has one partition index and no index type for a single column. */
    @Test
    void givenIceberg_whenOptimizeIndexes_thenKeepsOnePartitionIndex() {
      var queries =
          List.of(
              equalityAndRangeOn(EMAIL, COUNTRY),
              equalityAndRangeOn(COUNTRY, EMAIL),
              equalityAndRangeOn(CITY, EMAIL),
              equalityAndRangeOn(CITY, COUNTRY));
      var iceberg = new IndexSelectorConfigByDialect(JdbcDialect.Iceberg, defaults(8, 50));

      assertThat(selectIndexes(iceberg, queries)).hasSize(1);
    }

    private QueryIndexSummary equalityOn(int equalityColumn) {
      return new QueryIndexSummary(customers, Set.of(equalityColumn), Set.of(), Set.of(), 1.0);
    }

    private QueryIndexSummary equalityAndRangeOn(int equalityColumn, int inequalityColumn) {
      return new QueryIndexSummary(
          customers, Set.of(equalityColumn), Set.of(inequalityColumn), Set.of(), 1.0);
    }
  }

  /** A table with more filter patterns than {@code max-filter-patterns}. */
  @Nested
  class SingleColumnIndexes {

    private static final int MAX_FILTER_PATTERNS = 2;

    private final NamedTable customers = customersWith(TableStatistic.fromHint(1e6));

    @Test
    void givenColumnWithOnlyEqualityFilters_whenTooManyFilterPatterns_thenGetsHashIndex() {
      var queries = List.of(equalityOn(EMAIL), equalityOn(COUNTRY), equalityOn(EMAIL, COUNTRY));

      assertThat(selectIndexes(postgres(8, MAX_FILTER_PATTERNS), queries))
          .containsExactly("HASH[email]", "HASH[country]");
    }

    @Test
    void givenColumnWithInequalityFilter_whenTooManyFilterPatterns_thenGetsBtreeIndex() {
      var countryAndCityRange =
          new QueryIndexSummary(customers, Set.of(COUNTRY), Set.of(CITY), Set.of(), 1.0);
      var queries = List.of(equalityOn(EMAIL), equalityOn(CITY), countryAndCityRange);

      assertThat(selectIndexes(postgres(8, MAX_FILTER_PATTERNS), queries))
          .containsExactly("HASH[email]", "BTREE[city]", "HASH[country]");
    }

    private QueryIndexSummary equalityOn(Integer... equalityColumns) {
      return new QueryIndexSummary(customers, Set.of(equalityColumns), Set.of(), Set.of(), 1.0);
    }
  }

  private static final int EMAIL = 0;
  private static final int COUNTRY = 1;
  private static final int CITY = 2;
  private static final int ZIP = 3;
  private static final List<String> COLUMN_NAMES = List.of("email", "country", "city", "zip");

  private static List<String> selectIndexes(
      IndexSelectorConfig config, List<QueryIndexSummary> queries) {
    return selectIndexes(config, queries, ErrorCollector.root());
  }

  private static List<String> selectIndexes(
      IndexSelectorConfig config, List<QueryIndexSummary> queries, ErrorCollector errors) {
    var selector = new IndexSelector(null, config, Map.of(), errors);

    return selector.optimizeIndexes(queries).stream()
        .map(
            index ->
                index.getType()
                    + index.getColumns().stream().map(COLUMN_NAMES::get).toList().toString())
        .toList();
  }

  private static NamedTable customersWith(TableStatistic tableStatistic) {
    return customersWith(tableStatistic, List.of());
  }

  private static NamedTable customersWith(
      TableStatistic tableStatistic, List<Integer> primaryKeyColumns) {
    var rowType = mock(RelDataType.class);
    when(rowType.getFieldNames()).thenReturn(COLUMN_NAMES);
    for (var columnName : COLUMN_NAMES) {
      var field = mock(RelDataTypeField.class);
      when(field.getIndex()).thenReturn(COLUMN_NAMES.indexOf(columnName));
      when(rowType.getField(columnName, true, false)).thenReturn(field);
    }
    var relNode = mock(RelNode.class);
    when(relNode.getRowType()).thenReturn(rowType);
    var analysis =
        TableAnalysis.builder()
            .objectIdentifier(ObjectIdentifier.of("datasqrl", "public", "Customers"))
            .collapsedRelnode(relNode)
            .originalRelnode(relNode)
            .primaryKey(
                primaryKeyColumns.isEmpty()
                    ? PrimaryKeyMap.UNDEFINED
                    : PrimaryKeyMap.of(primaryKeyColumns))
            .tableStatistic(tableStatistic)
            .build();
    var createTableStatement = mock(CreateTableJdbcStatement.class);
    when(createTableStatement.getPrimaryKey())
        .thenReturn(primaryKeyColumns.stream().map(COLUMN_NAMES::get).toList());
    return new NamedTable("Customers", "Customers", analysis, createTableStatement);
  }

  private static IndexSelectorConfig postgres(int maxIndexes, int maxFilterPatterns) {
    return new IndexSelectorConfigByDialect(
        JdbcDialect.Postgres, defaults(maxIndexes, maxFilterPatterns));
  }

  /** The defaults of the {@code index-selection} settings, except for the given ones. */
  private static IndexSelectionConfig defaults(int maxIndexes, int maxFilterPatterns) {
    var indexSelection = mock(IndexSelectionConfig.class);
    when(indexSelection.getMaxIndexes()).thenReturn(maxIndexes);
    when(indexSelection.getMaxIndexColumns()).thenReturn(3);
    when(indexSelection.getMaxFilterPatterns()).thenReturn(maxFilterPatterns);
    when(indexSelection.getMinCostReduction()).thenReturn(0.05);
    when(indexSelection.getMinDistinctCount()).thenReturn(20);
    return indexSelection;
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
