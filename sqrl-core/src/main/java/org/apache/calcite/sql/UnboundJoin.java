package org.apache.calcite.sql;

import java.util.List;
import java.util.Optional;
import lombok.Getter;
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
    return null;
  }

  @NotNull
  @Override
  public List<SqlNode> getOperandList() {
    return null;
  }
}
