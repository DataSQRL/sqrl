package com.datasqrl.calcite.schema;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;

public class ShiftRex extends RexShuttle {

  private final int numToShift;
  private final RelNode relNode;

  /**
   * Shifts and adds type of new rel
   */
  public ShiftRex(int numToShift, RelNode relNode) {

    this.numToShift = numToShift;
    this.relNode = relNode;
  }

  @Override
  public RexNode visitInputRef(RexInputRef inputRef) {
    return new RexInputRef(
        inputRef.getIndex() + numToShift,
        relNode.getRowType().getFieldList().get(inputRef.getIndex() + numToShift).getType());
  }
}
