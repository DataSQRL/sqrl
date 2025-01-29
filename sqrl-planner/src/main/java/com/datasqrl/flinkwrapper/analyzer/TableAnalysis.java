package com.datasqrl.flinkwrapper.analyzer;


import com.datasqrl.flinkwrapper.analyzer.cost.CostAnalysis;
import com.datasqrl.flinkwrapper.tables.SourceTableAnalysis;
import com.datasqrl.io.tables.TableType;
import com.datasqrl.plan.rules.EngineCapability;
import com.datasqrl.plan.util.PrimaryKeyMap;
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
   * Whether this table is a distinct/deduplication table that only deduplicates
   * a CDC stream into the original state table. This is flagged so it can be optimized out.
   */
  @Builder.Default
  boolean hasMostRecentDistinct = false;
  /**
   * For stream tables that are unnested, we keep track of the root table in order
   * to detect when a join on the same root happens for optimization purposes
   */
  @Builder.Default @Exclude
  Optional<TableAnalysis> streamRoot = Optional.empty();
  /**
   * The tables and functions that occur in FROM clauses.
   * This is mutually exclusive with sourceTable below.
   */
  @Builder.Default @Exclude
  List<TableOrFunctionAnalysis> fromTables = List.of(); //Present for derived tables/views
  /**
   * If this table/function represents a source table (i.e. a CREATE TABLE) and not a view,
   * it is captured here.
   */
  @Builder.Default @Exclude
  Optional<SourceTableAnalysis> sourceTable = Optional.empty(); //Present for created source tables
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

  public Optional<TableAnalysis> getStreamRoot() {
    if (streamRoot == null && primaryKey.isDefined()) return Optional.of(this);
    return streamRoot;
  }

  public PrimaryKeyMap getSimplePrimaryKey() {
    return primaryKey.makeSimple(getRowType());
  }

  public boolean isSource() {
    return sourceTable.isPresent();
  }

  public RelNode getRelNode() {
    return collapsedRelnode;
  }

  public static TableAnalysis of(
      @NonNull ObjectIdentifier identifier,
      @NonNull SourceTableAnalysis sourceTable,
      @NonNull TableType type,
      @NonNull PrimaryKeyMap primaryKey) {
    return TableAnalysis.builder()
        .identifier(identifier)
        .collapsedRelnode(null)
        .originalRelnode(null)
        .type(type)
        .primaryKey(primaryKey)
        .sourceTable(Optional.of(sourceTable))
        .streamRoot(null)
        .build();
  }

  public RelNodeAnalysis toRelNode(RelNode relNode) {
    return new RelNodeAnalysis(relNode, type, primaryKey, getStreamRoot(), false);
  }

  public boolean matches(RelNode otherRelNode) {
    if (this.originalRelnode ==null) return false;
    return this.originalRelnode.deepEquals(otherRelNode);
  }

}
