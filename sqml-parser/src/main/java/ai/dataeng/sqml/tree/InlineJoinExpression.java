package ai.dataeng.sqml.tree;

import java.util.List;
import java.util.Optional;

public class InlineJoinExpression extends Expression {

  private final InlineJoin join;

  public InlineJoinExpression(Optional<NodeLocation> location,
      InlineJoin join) {
    super(location);
    this.join = join;
  }

  public InlineJoin getJoin() {
    return join;
  }

  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitInlineJoinExpression(this, context);
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
  public String toString() {
    return null;
  }
}
