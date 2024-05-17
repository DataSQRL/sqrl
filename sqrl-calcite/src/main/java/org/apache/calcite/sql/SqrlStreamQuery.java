package org.apache.calcite.sql;

import com.datasqrl.model.StreamType;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.calcite.sql.parser.SqlParserPos;

@Getter
public class SqrlStreamQuery extends SqrlSqlQuery {

  private final StreamType type;

  public SqrlStreamQuery(SqlParserPos location, Optional<SqlNodeList> hints,
      SqlIdentifier identifier, Optional<SqrlTableFunctionDef> tableArgs, SqlNode query,
      StreamType type) {
    super(location, hints, identifier, tableArgs, query);
    this.type = type;
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visit(this, context);
  }
}
