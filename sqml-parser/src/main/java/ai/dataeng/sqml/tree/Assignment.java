package ai.dataeng.sqml.tree;

import java.util.Optional;

public abstract class Assignment extends Node {

  protected Assignment(Optional<NodeLocation> location) {
    super(location);
  }

  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitAssignment(this, context);
  }
}
