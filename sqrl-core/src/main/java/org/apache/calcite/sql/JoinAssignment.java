package org.apache.calcite.sql;

import ai.datasqrl.parse.tree.name.NamePath;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import lombok.Getter;
import org.apache.calcite.sql.parser.SqlParserPos;

@Getter
public class JoinAssignment extends Assignment {

  private final SqlNode query;
  private final SqlNodeList hints;

  public JoinAssignment(SqlParserPos location,
      NamePath name, SqlNode query, SqlNodeList hints) {
    super(location, name);
    this.query = query;
    this.hints = hints;
  }

//  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
//    return visitor.visitJoinAssignment(this, context);
//  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    JoinAssignment that = (JoinAssignment) o;
    return Objects.equals(query, that.query) && Objects.equals(hints, that.hints);
  }

  @Override
  public int hashCode() {
    return Objects.hash(query, hints);
  }
}
