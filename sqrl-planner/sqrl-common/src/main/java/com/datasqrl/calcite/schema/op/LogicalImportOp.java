package com.datasqrl.calcite.schema.op;

import com.datasqrl.calcite.validator.ScriptValidator.QualifiedImport;
import java.util.List;
import java.util.Optional;
import lombok.Getter;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.sql.SqrlImportDefinition;

@Getter
public class LogicalImportOp extends AbstractRelNode implements LogicalOp {

  private final List<String> importPath;
  private final Optional<String> alias;
  private final List<QualifiedImport> importList;


  public LogicalImportOp(RelOptCluster cluster, RelTraitSet traitSet, List<String> importPath,
      Optional<String> alias, List<QualifiedImport> importList) {
    super(cluster, traitSet);
    this.importPath = importPath;
    this.alias = alias;
    this.importList = importList;
  }

  @Override
  public <R, C> R accept(LogicalOpVisitor<R, C> visitor, C context) {
    return visitor.visit(this, context);
  }
}