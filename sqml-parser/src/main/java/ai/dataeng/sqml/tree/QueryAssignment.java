package ai.dataeng.sqml.tree;

import java.util.List;
import java.util.Optional;

public class QueryAssignment extends Assignment {

  private final QualifiedName name;
  private final Query query;

  public QueryAssignment(Optional<NodeLocation> location, QualifiedName name,
      Query query) {
    super(location);
    this.name = name;
    this.query = query;
  }

  @Override
  public QualifiedName getName() {
    return name;
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

  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitQueryAssignment(this, context);
  }

  public Query getQuery() {
    return query;
  }
}
