package ai.datasqrl.parse.tree;

import ai.datasqrl.parse.tree.name.NamePath;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import lombok.Getter;

@Getter
public class ExpressionAssignment extends Assignment {

  private final String sql;
  private final List<Hint> hints;

  public ExpressionAssignment(Optional<NodeLocation> location,
      NamePath name, String sql, List<Hint> hints) {
    super(location, name);
    this.sql = sql;
    this.hints = hints;
  }

  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitExpressionAssignment(this, context);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ExpressionAssignment that = (ExpressionAssignment) o;
    return Objects.equals(sql, that.sql) && Objects.equals(hints, that.hints);
  }

  @Override
  public int hashCode() {
    return Objects.hash(sql, hints);
  }
}
