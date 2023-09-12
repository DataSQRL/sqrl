package org.apache.calcite.sql;

import java.util.List;
import java.util.Optional;
import lombok.Getter;
import org.apache.calcite.sql.parser.SqlParserPos;

@Getter
public class SqrlDistinctQuery extends SqrlAssignment {

  private final SqlNode table;
  private final List<SqlNode> operands;
  private final List<SqlNode> order;

  public SqrlDistinctQuery(SqlParserPos location, Optional<SqlNodeList> hints,
      SqlIdentifier identifier, Optional<SqrlTableFunctionDef> tableArgs, SqlNode table,
      List<SqlNode> operands, List<SqlNode> order) {
    super(location, hints, identifier, tableArgs);
    this.table = table;
    this.operands = operands;
    this.order = order;
  }
}
