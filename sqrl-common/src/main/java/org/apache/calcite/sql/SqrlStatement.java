package org.apache.calcite.sql;

import com.datasqrl.name.NamePath;
import java.util.Optional;
import lombok.Getter;
import org.apache.calcite.sql.parser.SqlParserPos;

@Getter
public abstract class SqrlStatement extends SqlNode {

  protected final SqlIdentifier identifier;
  protected final NamePath namePath;
  protected final Optional<SqlNodeList> hints;

  protected SqrlStatement(SqlParserPos location, SqlIdentifier identifier, NamePath namePath,
      Optional<SqlNodeList> hints) {
    super(location);
    this.identifier = identifier;
    this.namePath = namePath;
    this.hints = hints;
  }

  public abstract <R, C> R accept(StatementVisitor<R, C> visitor, C context);
}
