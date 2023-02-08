package org.apache.calcite.sql.type;

import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.flink.calcite.shaded.com.google.common.collect.ImmutableList;

public class SqrlExplicitOperandTypeInference extends ExplicitOperandTypeInference {

  public SqrlExplicitOperandTypeInference(
      List<RelDataType> paramTypes) {
    super(ImmutableList.copyOf(paramTypes));
  }
}
