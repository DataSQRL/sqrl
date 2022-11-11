package org.apache.calcite.sql;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import lombok.Getter;
import org.apache.calcite.sql.SqlJoin.SqlJoinOperator;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.jetbrains.annotations.NotNull;

/**
 * Represents a partial join
 * e.g.
 * JOIN X ON y
 */
@Getter
public class UnboundJoin extends SqlCall {
  SqlNode relation;
  Optional<SqlNode> condition;

  public UnboundJoin(SqlParserPos pos, SqlNode relation,
      Optional<SqlNode> condition) {
    super(pos);
    this.relation = relation;
    this.condition = condition;
  }

  @Override
  public SqlKind getKind() {
    return SqlKind.UNBOUND_JOIN;
  }

  @NotNull
  @Override
  public SqlOperator getOperator() {
    //this isn't the true operator and will fail if you try to rewrite child items in the shuttle
    return SqrlJoinDeclarationSpec.OPERATOR;
  }

  @NotNull
  @Override
  public List<SqlNode> getOperandList() {
    return List.of();
  }
}
