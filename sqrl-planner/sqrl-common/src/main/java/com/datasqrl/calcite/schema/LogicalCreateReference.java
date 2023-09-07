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

  final String name;
  private final TableFunction fnc;
  private final List<String> fromPath;
  private final List<String> toPath;
  private final SqrlTableFunctionDef argNames;

  public LogicalCreateReference(RelOptCluster cluster, RelTraitSet traitSet,
      List<String> fromPath, List<String> toPath,
      String name, SqrlTableFunctionDef argNames, RelNode relNode, TableFunction fnc) {
    super(cluster, traitSet, null, relNode);
    this.fromPath = fromPath;
    this.toPath = toPath;
    this.argNames = argNames;
    this.name = name;
    this.fnc = fnc;
  }

  @Override
  public <R, C> R accept(LogicalOpVisitor<R, C> visitor, C context) {
    return visitor.visit(this, context);
  }

}