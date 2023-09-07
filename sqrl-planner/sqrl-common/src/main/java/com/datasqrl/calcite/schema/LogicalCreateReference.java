package com.datasqrl.calcite.schema;

import java.util.List;
import lombok.Getter;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.TableFunction;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqrlTableFunctionDef;

@Getter
public class LogicalCreateReference extends LogicalCreateAlias implements LogicalOp {


  private final List<String> fromPath;
  private final List<List<String>> tableReferences;
  private final SqrlTableFunctionDef def;

  public LogicalCreateReference(RelOptCluster cluster, RelTraitSet traitSet,
      List<String> fromPath, List<List<String>> tableReferences, SqrlTableFunctionDef def, RelNode relNode) {
    super(cluster, traitSet, null, relNode);
    this.fromPath = fromPath;
    this.tableReferences = tableReferences;
    this.def = def;
  }

  @Override
  public <R, C> R accept(LogicalOpVisitor<R, C> visitor, C context) {
    return visitor.visit(this, context);
  }

}