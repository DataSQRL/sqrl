package ai.datasqrl.plan.local.transpiler.nodes.relation;

import ai.datasqrl.parse.tree.AstVisitor;
import ai.datasqrl.parse.tree.Expression;
import ai.datasqrl.parse.tree.Join;
import ai.datasqrl.parse.tree.JoinDeclaration;
import ai.datasqrl.parse.tree.Limit;
import ai.datasqrl.parse.tree.Node;
import ai.datasqrl.parse.tree.NodeLocation;
import ai.datasqrl.parse.tree.OrderBy;
import ai.datasqrl.parse.tree.Relation;
import ai.datasqrl.parse.tree.SortItem;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.schema.Field;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Delegate;

/**
 * An normalized version of {@link ai.datasqrl.parse.tree.JoinDeclaration}
 */
@Getter
@ToString
@EqualsAndHashCode(callSuper = false)
public class JoinDeclarationNorm
    extends RelationNorm {
  private final RelationNorm relation;

  private final Optional<Name> inverse;
  private final Optional<OrderBy> orderBy;
  private final Optional<Limit> limit;

  public JoinDeclarationNorm(Optional<NodeLocation> location, RelationNorm relation,
      Optional<OrderBy> orderBy, Optional<Limit> limit, Optional<Name> inverse) {
    super(location);
    this.relation = relation;
    this.orderBy = orderBy;
    this.limit = limit;
    this.inverse = inverse;
  }

  @Override
  public Name getFieldName(Expression references) {
    throw new RuntimeException("Not supported");
  }

  @Override
  public List<Expression> getFields() {
    throw new RuntimeException("Not supported");
  }

  @Override
  public List<Expression> getPrimaryKeys() {
    throw new RuntimeException("Not supported");
  }

  @Override
  public Optional<Expression> walk(NamePath namePath) {
    return Optional.empty();
  }

  @Override
  public RelationNorm getLeftmost() {
    return relation.getLeftmost();
  }

  @Override
  public RelationNorm getRightmost() {
    return relation.getRightmost();
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitJoinDeclarationNorm(this, context);
  }

  @Override
  public List<? extends Node> getChildren() {
    return List.of(relation);
  }
}
