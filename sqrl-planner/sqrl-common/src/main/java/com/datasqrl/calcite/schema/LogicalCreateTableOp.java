package com.datasqrl.calcite.schema;

import java.util.List;
import java.util.Optional;
import lombok.Getter;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqrlTableFunctionDef;

@Getter
public class LogicalCreateTableOp extends SingleRel implements LogicalOp {

  final List<RelHint> hints;
  final Optional<SqlNodeList> opHints;
  final List<String> path;
  final RelNode input;
  final boolean setFieldNames;
  final RelOptTable fromRelOptTable;
  final SqrlTableFunctionDef args;

  public LogicalCreateTableOp(RelOptCluster cluster, RelTraitSet traitSet, RelNode input,
      List<RelHint> hints, Optional<SqlNodeList> opHints, List<String> path, boolean setFieldNames,
      RelOptTable fromRelOptTable, SqrlTableFunctionDef args) {
    super(cluster, traitSet, input);
    this.input = input;
    this.hints = hints;
    this.opHints = opHints;
    this.path = path;
    this.setFieldNames = setFieldNames;
    this.fromRelOptTable = fromRelOptTable;
    this.args = args;
  }

  @Override
  public <R, C> R accept(LogicalOpVisitor<R, C> visitor, C context) {
    return visitor.visit(this, context);
  }
}