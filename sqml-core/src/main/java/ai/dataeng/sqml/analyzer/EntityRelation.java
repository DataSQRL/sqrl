package ai.dataeng.sqml.analyzer;

import ai.dataeng.sqml.sql.tree.Node;
import ai.dataeng.sqml.sql.tree.NodeLocation;
import ai.dataeng.sqml.sql.tree.Relation;
import java.util.List;
import java.util.Optional;

public class EntityRelation extends Relation {

  protected EntityRelation(Optional<NodeLocation> location) {
    super(location);
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
