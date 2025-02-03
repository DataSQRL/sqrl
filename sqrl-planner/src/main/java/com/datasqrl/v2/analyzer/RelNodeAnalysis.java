package com.datasqrl.v2.analyzer;

import com.datasqrl.io.tables.TableType;
import com.datasqrl.plan.rules.RelHolder;
import com.datasqrl.plan.util.PrimaryKeyMap;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import org.apache.calcite.rel.RelNode;

/**
 * Intermediate analysis used by the {@link SQRLLogicalPlanAnalyzer} to keep track of
 * information as it processes the relational operator tree of {@link RelNode}s.
 *
 * @see TableAnalysis for more information
 */
@Getter
@Builder(toBuilder = true)
@AllArgsConstructor
public class RelNodeAnalysis implements RelHolder, AbstractAnalysis {

  @NonNull
  RelNode relNode;
  @NonNull @Builder.Default
  TableType type = TableType.RELATION;
  @NonNull @Builder.Default
  PrimaryKeyMap primaryKey = PrimaryKeyMap.UNDEFINED;
  @Builder.Default
  Optional<TableAnalysis> streamRoot = Optional.empty();
  @Builder.Default
  boolean hasNowFilter = false;


}
