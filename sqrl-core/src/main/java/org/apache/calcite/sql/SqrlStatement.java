package org.apache.calcite.sql;

import java.util.Optional;
import org.apache.calcite.sql.parser.SqlParserPos;

public abstract class SqrlStatement extends SqlNode {

  protected SqrlStatement(SqlParserPos location) {
    super(location);
  }

}
