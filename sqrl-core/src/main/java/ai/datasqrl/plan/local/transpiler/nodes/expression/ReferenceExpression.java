package ai.datasqrl.plan.local.transpiler.nodes.expression;


import ai.datasqrl.parse.tree.AstVisitor;
import ai.datasqrl.parse.tree.Expression;
import ai.datasqrl.parse.tree.Node;
import ai.datasqrl.plan.local.transpiler.nodes.relation.RelationNorm;
import java.util.List;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;


/**
 * References a subquery column
 */
@Getter
public class ReferenceExpression extends Expression {
  @Setter
  private RelationNorm relationNorm;
  private final Expression references;

  public ReferenceExpression(RelationNorm relationNorm,
      @NonNull Expression references) {
    super(references.getLocation());
    this.relationNorm = relationNorm;
    this.references = references;
  }

  @Override
  public List<? extends Node> getChildren() {
    return null;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  @Override
  public boolean equals(Object obj) {
    return false;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitReferenceExpression(this, context);
  }
}
