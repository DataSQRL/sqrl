package com.datasqrl.v2.analyzer;


import com.datasqrl.error.ErrorCollector;
import com.datasqrl.plan.table.TableStatistic;
import com.datasqrl.v2.analyzer.cost.CostAnalysis;
import com.datasqrl.v2.hint.PlannerHints;
import com.datasqrl.v2.tables.SourceSinkTableAnalysis;
import com.datasqrl.io.tables.TableType;
import com.datasqrl.plan.rules.EngineCapability;
import com.datasqrl.plan.util.PrimaryKeyMap;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.EqualsAndHashCode.Include;
import lombok.NonNull;
import lombok.ToString.Exclude;
import lombok.Value;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.flink.table.catalog.ObjectIdentifier;

/**
 * The analysis of a planned table or function definition.
 * It specifies important information about the table/function that is used during planning and
 * subsequent stages of compilation.
 */
@Builder
@Value
@AllArgsConstructor
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class TableAnalysis implements TableOrFunctionAnalysis {

  /**
   * The unique identifier of this table within Flink's catalog.
   */
  @NonNull @Include
  ObjectIdentifier identifier;
  /**
   * The collapsed/deduplicated Relnode which undoes the view expansion that
   * Flink does during planning.
   */
  RelNode collapsedRelnode;
  /**
   * The original Relnode produced by the Flink planner.
   */
  RelNode originalRelnode;
  /**
   * The original SQL if available, else an empty string
   */
  @NonNull @Builder.Default
  String originalSql = "";
  /**
   * The type of the table/function
   */
  @NonNull @Builder.Default
  TableType type = TableType.RELATION;
  /**
   * The inferred primary key of the table
   */
  @NonNull @Builder.Default
  PrimaryKeyMap primaryKey = PrimaryKeyMap.UNDEFINED;
  /**
   * If this table selects from and has the identical rowtype to
   * one of it's input tables we consider that table to be the base table.
   * We keep track of this so we don't generate a bunch of identical types in the API.
   */
  @NonNull @Builder.Default
  Optional<TableAnalysis> optionalBaseTable = Optional.empty();
  /**
   * Whether this table is a distinct/deduplication table that only deduplicates
   * a CDC stream into the original state table. This is flagged so it can be optimized out in the DAG
   */
  @Builder.Default
  boolean isMostRecentDistinct = false;
  /**
   * For stream tables that are unnested, we keep track of the root table in order
   * to detect when a join on the same root happens for optimization purposes
   */
  @Builder.Default @Exclude
  Optional<TableAnalysis> streamRoot = Optional.empty();
  /**
   * The top-level sort for this table from the original query definition.
   * It was extracted during planning since sorting doesn't make sense on the stream
   * and is added to the queries during DAG planning. This is a performance optimization.
   */
  @Builder.Default @Exclude
  Optional<Sort> topLevelSort = Optional.empty();
  /**
   * The tables and functions that occur in FROM clauses.
   * This is mutually exclusive with sourceTable below.
   */
  @Builder.Default @Exclude
  List<TableOrFunctionAnalysis> fromTables = List.of(); //Present for derived tables/views
  /**
   * If this table/function represents a source or sink table (i.e. an explicit CREATE TABLE with connector definition)
   * and not a view, it is captured here.
   */
  @Builder.Default @Exclude
  Optional<SourceSinkTableAnalysis> sourceSinkTable = Optional.empty(); //Present for created source tables
  /**
   * The required {@link EngineCapability} needed to execute this query
   */
  @Builder.Default
  Set<EngineCapability> requiredCapabilities = Set.of();
  /**
   * Cost analyses used by the planner to determine which engine should execute this query
   */
  @Builder.Default
  List<CostAnalysis> costs = List.of();

  /**
   * The planner hints attached to this table definition
   */
  @Builder.Default
  PlannerHints hints = PlannerHints.EMPTY;

  /**
   * The error collector for the corresponding table definition
   * for when we need to produce table specific errors
   */
  @Builder.Default
  ErrorCollector errors = ErrorCollector.root();

  public Optional<TableAnalysis> getStreamRoot() {
    if (streamRoot == null) {
      if (primaryKey.isDefined() && type==TableType.STREAM) {
        return Optional.of(this);
      } else {
        return Optional.empty();
      }
    }
    return streamRoot;
  }

  @Override
  public TableAnalysis getBaseTable() {
    return optionalBaseTable.orElse(this);
  }

  public String getName() {
    return identifier.getObjectName();
  }

  public PrimaryKeyMap getSimplePrimaryKey() {
    return primaryKey.makeSimple(getRowType());
  }

  public boolean isSourceOrSink() {
    return sourceSinkTable.isPresent();
  }

  /**
   * Returns the statistics for this table.
   * TODO: this is currently hardcoded and should be
   * produced by analyzer and extracted from table definition (if available) on import
   * @return
   */
  public TableStatistic getTableStatistic() {
    return TableStatistic.of(10000);
  }


  public RelNode getRelNode() {
    return collapsedRelnode;
  }

  public static TableAnalysis of(
      @NonNull ObjectIdentifier identifier,
      @NonNull SourceSinkTableAnalysis sourceTable,
      @NonNull TableType type,
      @NonNull PrimaryKeyMap primaryKey) {
    return TableAnalysis.builder()
        .identifier(identifier)
        .collapsedRelnode(null)
        .originalRelnode(null)
        .type(type)
        .primaryKey(primaryKey)
        .sourceSinkTable(Optional.of(sourceTable))
        .streamRoot(null)
        .build();
  }

  public RelNodeAnalysis toRelNode(RelNode relNode) {
    return new RelNodeAnalysis(relNode, type, primaryKey, getStreamRoot(), false);
  }

  @Override
  public List<String> getParameterNames() {
    return Collections.EMPTY_LIST;
  }
}
