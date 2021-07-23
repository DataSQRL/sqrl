package ai.dataeng.sqml.tree;

import java.util.List;
import java.util.Optional;

public class JoinSubexpression extends Expression {

  private final TraversalJoin join;

  public JoinSubexpression(Optional<NodeLocation> location,
      TraversalJoin join) {
    super(location);
    this.join = join;
  }

  public TraversalJoin getJoin() {
    return join;
  }

  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitJoinSubexpression(this, context);
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
