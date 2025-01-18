package com.datasqrl.flinkwrapper.analyzer;


import com.datasqrl.flinkwrapper.hint.PlannerHints;
import com.datasqrl.io.tables.TableType;
import com.datasqrl.plan.rules.EngineCapability;
import com.datasqrl.plan.util.PrimaryKeyMap;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import lombok.ToString.Exclude;
import lombok.Value;
import org.apache.calcite.rel.RelNode;
import org.apache.flink.table.catalog.ObjectIdentifier;

@Builder
@Value
@AllArgsConstructor
public class TableAnalysis {

  @NonNull
  ObjectIdentifier identifier;
  @NonNull
  RelNode relNode;
  @NonNull @Builder.Default
  TableType type = TableType.RELATION;
  @NonNull @Builder.Default
  PrimaryKeyMap primaryKey = PrimaryKeyMap.UNDEFINED;
  @Builder.Default
  boolean isMostRecentDistinct = false;
  @Builder.Default @Exclude
  Optional<TableAnalysis> streamRoot = Optional.empty();
  @Builder.Default
  List<ObjectIdentifier> sourceTables = List.of();
  @Builder.Default
  Set<EngineCapability> requiredCapabilities = Set.of();
  @Builder.Default
  PlannerHints hints = PlannerHints.EMPTY;

  public Optional<TableAnalysis> getStreamRoot() {
    if (streamRoot == null && primaryKey.isDefined()) return Optional.of(this);
    return streamRoot;
  }

  public static TableAnalysis of(
      @NonNull ObjectIdentifier identifier,
      @NonNull RelNode relNode,
      @NonNull TableType type,
      @NonNull PrimaryKeyMap primaryKey) {
    return TableAnalysis.builder()
        .identifier(identifier)
        .relNode(relNode)
        .type(type)
        .primaryKey(primaryKey)
        .streamRoot(null)
        .build();
  }

  public RelNodeAnalysis toRelNode(RelNode relNode) {
    return new RelNodeAnalysis(relNode, type, primaryKey, isMostRecentDistinct, getStreamRoot(), false);
  }

}
