package org.apache.calcite.sql;

import java.util.Optional;
import lombok.Getter;
import org.apache.calcite.sql.parser.SqlParserPos;

@Getter
public abstract class SqrlStatement extends SqlNode {
  protected final SqlIdentifier namePath;
  protected final Optional<SqlNodeList> hints;

  protected SqrlStatement(SqlParserPos location, SqlIdentifier namePath, Optional<SqlNodeList> hints) {
    super(location);
    this.namePath = namePath;
    this.hints = hints;
  }

}
