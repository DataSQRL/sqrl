package org.apache.calcite.sql;

import ai.datasqrl.parse.tree.name.NamePath;
import java.util.Optional;
import lombok.Getter;
import org.apache.calcite.sql.parser.SqlParserPos;

@Getter
public abstract class SqrlStatement extends SqlNode {
  protected final NamePath namePath;

  protected SqrlStatement(SqlParserPos location, NamePath namePath) {
    super(location);
    this.namePath = namePath;
  }

}
