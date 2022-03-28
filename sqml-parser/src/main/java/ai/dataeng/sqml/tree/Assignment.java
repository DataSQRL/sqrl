package ai.dataeng.sqml.tree;

import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.NamePath;
import java.util.Optional;

public abstract class Assignment extends SqrlStatement {

  private final QualifiedName name;

  protected Assignment(Optional<NodeLocation> location, QualifiedName name) {
    super(location);
    this.name = name;
  }

  public QualifiedName getName() {
    return name;
  }

  public NamePath getNamePath() {
    return name.toNamePath();
  }

  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitAssignment(this, context);
  }
}
