package org.apache.calcite.sql;

import java.util.List;
import java.util.Optional;
import lombok.Getter;
import org.apache.calcite.sql.parser.SqlParserPos;

/**
 * Represents a partial join e.g. JOIN X ON y
 */
@Getter
public class UnboundJoin extends SqlCall {

  SqlNode relation;
  Optional<SqlNode> condition;

  public static final SqlSpecialOperator OPERATOR = new Operator() {
    public SqlCall createCall(SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
      return new SqrlJoinDeclarationSpec(pos, (SqrlJoinTerm) operands[0],
          Optional.ofNullable((SqlNodeList) operands[1]),
          (Optional) Optional.ofNullable(operands[2]),
          (Optional) Optional.ofNullable(operands[3]),
          (Optional) Optional.ofNullable(operands[4]));
    }
  };
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

  @Override
  public SqlOperator getOperator() {
    //this isn't the true operator and will fail if you try to rewrite child items in the shuttle
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return List.of();
  }

  private static class Operator extends SqlSpecialOperator {

    private Operator() {
      super("UNBOUND JOIN", SqlKind.OTHER, 0);
    }

    public SqlSyntax getSyntax() {
      return SqlSyntax.POSTFIX;
    }

    @Override
    public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
      UnboundJoin join = (UnboundJoin) call;
      // Unparse the relation
      writer.keyword("JOIN");
      join.getOperandList().get(0).unparse(writer, leftPrec, rightPrec);

      // Unparse the condition if it is present
      Optional<SqlNode> condition = join.getCondition();
      if (condition.isPresent()) {
        writer.keyword("ON");
        condition.get().unparse(writer, leftPrec, rightPrec);
      }
    }
  }
}
