package ai.dataeng.sqml.tree;

import java.util.List;
import java.util.Optional;

public class RelationshipJoin extends Expression {

  private final Join join;

  public RelationshipJoin(Optional<NodeLocation> location,
      Join join) {
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
}
