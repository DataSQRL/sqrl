package ai.dataeng.sqml.tree;

import ai.dataeng.sqml.tree.name.NamePath;
import java.util.List;
import java.util.Optional;

public class QueryAssignment extends Assignment {

  private final Query query;
  private final NamePath namePath;
  private final String sql;

  public QueryAssignment(Optional<NodeLocation> location, NamePath namePath,
      Query query, String sql) {
    super(location, null);
    this.query = query;
    this.namePath = namePath;
    this.sql = sql;
  }

  public String getSql() {
    return sql;
  }

  @Override
  public NamePath getNamePath() {
    return namePath;
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
