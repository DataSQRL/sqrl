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

  @Override
  public void unparse(SqlWriter sqlWriter, int i, int i1) {
    super.unparse(sqlWriter, i, i1);
    sqlWriter.keyword("DISTINCT");
    table.unparse(sqlWriter, i, i1);
    sqlWriter.print("ON ...");
  }

  @Override
  public <R, C> R accept(SqrlStatementVisitor<R, C> visitor, C context) {
    return visitor.visit(this, context);
  }
}
