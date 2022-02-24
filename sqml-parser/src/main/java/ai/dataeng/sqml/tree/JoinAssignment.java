package ai.dataeng.sqml.tree;

import java.util.List;
import java.util.Optional;

public class JoinAssignment extends Assignment {

  private final String query;
  private final InlineJoin inlineJoin;

  public JoinAssignment(Optional<NodeLocation> location,
      QualifiedName name, String query, InlineJoin inlineJoin) {
    super(location, name);
    this.query = query;

    this.inlineJoin = inlineJoin;
  }

  public InlineJoin getInlineJoin() {
    return inlineJoin;
  }

  public String getQuery() {
    return query;
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
}
