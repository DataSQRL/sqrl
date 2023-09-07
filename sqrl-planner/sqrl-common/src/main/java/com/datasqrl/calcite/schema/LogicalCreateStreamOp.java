package com.datasqrl.calcite.schema;

import com.datasqrl.plan.rel.LogicalStream;
import java.util.List;
import java.util.Optional;
import lombok.Getter;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqrlTableFunctionDef;

@Getter
public class LogicalCreateStreamOp extends LogicalCreateTableOp implements LogicalOp {

  final LogicalStream input;

  public LogicalCreateStreamOp(RelOptCluster cluster, RelTraitSet traitSet, LogicalStream input,
      List<String> path, RelOptTable fromRelOptTable, Optional<SqlNodeList> opHints,
      SqrlTableFunctionDef args) {
    super(cluster, traitSet, input, List.of(), opHints, path, true, fromRelOptTable, args);
    this.input = input;
  }

  @Override
  public <R, C> R accept(LogicalOpVisitor<R, C> visitor, C context) {
    return visitor.visit(this, context);
  }
}
