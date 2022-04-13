package ai.dataeng.sqml.tree;

import ai.dataeng.sqml.tree.name.NamePath;
import java.util.Optional;

public abstract class Assignment extends SqrlStatement {

  private final NamePath name;

  protected Assignment(Optional<NodeLocation> location, NamePath name) {
    super(location);
    this.name = name;
  }

  public NamePath getNamePath() {
    return name;
  }

  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitAssignment(this, context);
  }
}
