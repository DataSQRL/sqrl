package com.datasqrl.flinkwrapper.analyzer;

import com.datasqrl.engine.ExecutionEngine.Base;
import com.datasqrl.io.tables.TableType;
import com.datasqrl.plan.rules.RelHolder;
import com.datasqrl.plan.util.PrimaryKeyMap;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;

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
