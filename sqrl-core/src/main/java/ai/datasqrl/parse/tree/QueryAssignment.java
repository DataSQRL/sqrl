package ai.datasqrl.parse.tree;

import ai.datasqrl.parse.tree.name.NamePath;
import java.util.List;
import java.util.Optional;

public class QueryAssignment extends Assignment {

  private final Query query;
  private final String sql;
  private final List<Hint> hints;

  public QueryAssignment(Optional<NodeLocation> location, NamePath namePath,
      Query query, String sql, List<Hint> hints) {
    super(location, namePath);
    this.query = query;
    this.sql = sql;
    this.hints = hints;
  }

  public String getSql() {
    return sql;
  }

  public List<Hint> getHints() {
    return hints;
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
