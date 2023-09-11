package com.datasqrl.calcite.schema.op;

import com.datasqrl.calcite.validator.ScriptValidator.QualifiedExport;
import java.util.List;
import lombok.Getter;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;

@Getter
public class LogicalExportOp extends SingleRel implements LogicalOp {

  final QualifiedExport export;

  public LogicalExportOp(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, QualifiedExport export) {
    super(cluster, traitSet, input);
    this.export = export;
  }

  @Override
  public RelOptTable getTable() {
    //always wrapped in a project
    return input.getInput(0).getTable();
  }

  @Override
  public <R, C> R accept(LogicalOpVisitor<R, C> visitor, C context) {
    return visitor.visit(this, context);
  }
}
