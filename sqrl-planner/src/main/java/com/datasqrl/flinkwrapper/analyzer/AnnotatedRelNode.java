package com.datasqrl.flinkwrapper.analyzer;

import com.datasqrl.io.tables.TableType;
import com.datasqrl.plan.global.SqrlDAG.TableNode;
import com.datasqrl.plan.rules.RelHolder;
import com.datasqrl.plan.table.NowFilter;
import com.datasqrl.plan.util.PrimaryKeyMap;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import org.apache.calcite.rel.RelNode;

@Value
@Builder
@AllArgsConstructor
public class AnnotatedRelNode implements RelHolder {

  @NonNull
  RelNode relNode;
  @NonNull
  TableType type;
  @NonNull
  PrimaryKeyMap primaryKey;
  boolean isMostRecentDistinct;
  @Builder.Default
  NowFilter nowFilter = NowFilter.EMPTY;
  @Builder.Default
  Optional<TableNode> streamRoot = Optional.empty();

}
