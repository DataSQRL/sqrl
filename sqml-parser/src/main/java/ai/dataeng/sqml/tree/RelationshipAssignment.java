package ai.dataeng.sqml.tree;

import java.util.List;
import java.util.Optional;

public class RelationshipAssignment extends Assignment {

  private final RelationshipJoin join;

  public RelationshipAssignment(Optional<NodeLocation> location, RelationshipJoin join) {
    super(location);
    this.join = join;
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

  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitRelationshipAssignment(this, context);
  }
  public RelationshipJoin getJoin() {
    return join;
  }
}
