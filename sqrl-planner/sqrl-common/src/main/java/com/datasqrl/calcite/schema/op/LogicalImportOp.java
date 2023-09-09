package com.datasqrl.calcite.schema.op;

import java.util.List;
import java.util.Optional;
import lombok.Getter;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;

@Getter
public class LogicalImportOp extends AbstractRelNode implements LogicalOp {

  private final List<String> importPath;
  private final Optional<String> alias;

  public LogicalImportOp(RelOptCluster cluster, RelTraitSet traitSet, List<String> importPath,
      Optional<String> alias) {
    super(cluster, traitSet);
    this.importPath = importPath;
    this.alias = alias;
  }

  @Override
  public <R, C> R accept(LogicalOpVisitor<R, C> visitor, C context) {
    return visitor.visit(this, context);
  }
}