package com.datasqrl.flinkwrapper.analyzer;


import com.datasqrl.flinkwrapper.analyzer.cost.CostAnalysis;
import com.datasqrl.flinkwrapper.hint.PlannerHints;
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
import org.apache.flink.table.types.DataType;

@Builder
@Value
@AllArgsConstructor
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class TableAnalysis implements AbstractAnalysis {

  @NonNull @Include
  ObjectIdentifier identifier;
  RelNode collapsedRelnode;
  RelNode originalRelnode;
  DataType flinkDataType;
  @NonNull @Builder.Default
  TableType type = TableType.RELATION;
  @NonNull @Builder.Default
  PrimaryKeyMap primaryKey = PrimaryKeyMap.UNDEFINED;
  @Builder.Default
  boolean hasMostRecentDistinct = false;
  @Builder.Default @Exclude
  Optional<TableAnalysis> streamRoot = Optional.empty();
  @Builder.Default @Exclude
  List<TableAnalysis> fromTables = List.of(); //Present for derived tables/views
  @Builder.Default @Exclude
  Optional<SourceTableAnalysis> sourceTable = Optional.empty(); //Present for created source tables
  @Builder.Default
  Set<EngineCapability> requiredCapabilities = Set.of();
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
