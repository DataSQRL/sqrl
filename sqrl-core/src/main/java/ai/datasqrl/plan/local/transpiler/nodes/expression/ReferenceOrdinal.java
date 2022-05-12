package ai.datasqrl.plan.local.transpiler.nodes.expression;

import ai.datasqrl.parse.tree.AstVisitor;
import ai.datasqrl.parse.tree.Identifier;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.plan.local.transpiler.nodes.relation.QuerySpecNorm;
import lombok.Getter;
import lombok.Setter;

@Getter
public class ReferenceOrdinal extends Identifier {
  @Setter
  QuerySpecNorm table;
  int ordinal;

  public ReferenceOrdinal(int ordinal) {
    super(Name.system(ordinal + ""));
    this.ordinal = ordinal;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitReferenceOrdinal(this, context);
  }
}