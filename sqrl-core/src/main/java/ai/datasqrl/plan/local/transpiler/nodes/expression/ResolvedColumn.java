package ai.datasqrl.plan.local.transpiler.nodes.expression;

import ai.datasqrl.parse.tree.AstVisitor;
import ai.datasqrl.parse.tree.Expression;
import ai.datasqrl.parse.tree.Identifier;
import ai.datasqrl.plan.local.transpiler.nodes.relation.RelationNorm;
import ai.datasqrl.schema.Column;
import lombok.Getter;

@Getter
public class ResolvedColumn extends Identifier {

  private final Identifier identifier;
  RelationNorm relationNorm;
  Column column;

  public ResolvedColumn(Identifier identifier,
      RelationNorm relationNorm,
    Column column) {
    super(identifier.getLocation(), identifier.getNamePath());
    this.identifier = identifier;
    this.relationNorm = relationNorm;
    this.column = column;
  }

  public static ResolvedColumn of(RelationNorm norm, Column column) {
    return new ResolvedColumn(new Identifier(column.getId()), norm, column);
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitResolvedColumn(this, context);
  }
}
