package ai.datasqrl.parse.tree;

import ai.datasqrl.parse.tree.name.NamePath;
import java.util.List;
import java.util.Optional;

public class JoinAssignment extends Assignment {

  private final String query;
  private final JoinDeclaration joinDeclaration;
  private final List<Hint> hints;

  public JoinAssignment(Optional<NodeLocation> location,
      NamePath name, String query, JoinDeclaration joinDeclaration, List<Hint> hints) {
    super(location, name);
    this.query = query;

    this.joinDeclaration = joinDeclaration;
    this.hints = hints;
  }

  public JoinDeclaration getJoinDeclaration() {
    return joinDeclaration;
  }

  public String getQuery() {
    return query;
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
    return visitor.visitJoinAssignment(this, context);
  }

  public String getSqlQuery() {
    //add hints
    return "SELECT * FROM _ " + getQuery();
  }
}
