package org.apache.calcite.sql;

import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.schema.TableFunctionArgument;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import lombok.Getter;
import org.apache.calcite.sql.parser.SqlParserPos;

@Getter
public class StreamAssignment extends QueryAssignment {

  private final Optional<List<TableFunctionArgument>> tableArgs;
  private final SqlNode query;
  private final SubscriptionType type;

  public StreamAssignment(SqlParserPos location, NamePath namePath,
      Optional<List<TableFunctionArgument>> tableArgs, SqlNode query, SubscriptionType type,
      Optional<SqlNodeList> hints) {
    super(location, namePath, tableArgs, query, hints);
    this.tableArgs = tableArgs;
    this.query = query;
    this.type = type;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    StreamAssignment that = (StreamAssignment) o;
    return Objects.equals(query, that.query) && Objects.equals(hints, that.hints);
  }

  @Override
  public int hashCode() {
    return Objects.hash(query, hints);
  }

  @Override
  public void unparse(SqlWriter sqlWriter, int i, int i1) {
    super.unparse(sqlWriter, i, i1);
  }
}
