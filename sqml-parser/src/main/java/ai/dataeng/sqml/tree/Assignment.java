package ai.dataeng.sqml.tree;

import java.util.Optional;

public abstract class Assignment extends Node {

  private final QualifiedName name;

  protected Assignment(Optional<NodeLocation> location, QualifiedName name) {
    super(location);
    this.name = name;
  }

  public QualifiedName getName() {
    return name;
  }

  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitAssignment(this, context);
  }
}
