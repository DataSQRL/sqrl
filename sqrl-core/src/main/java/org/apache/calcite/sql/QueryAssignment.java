package org.apache.calcite.sql;

import ai.datasqrl.parse.tree.name.NamePath;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import lombok.Getter;
import org.apache.calcite.sql.parser.SqlParserPos;

@Getter
public class QueryAssignment extends Assignment {

  private final SqlNode query;
  private final SqlNodeList hints;

  public QueryAssignment(SqlParserPos location, NamePath namePath,
      SqlNode query, SqlNodeList hints) {
    super(location, namePath);
    this.query = query;
    this.hints = hints;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    QueryAssignment that = (QueryAssignment) o;
    return Objects.equals(query, that.query) && Objects.equals(hints, that.hints);
  }

  @Override
  public int hashCode() {
    return Objects.hash(query, hints);
  }
}
