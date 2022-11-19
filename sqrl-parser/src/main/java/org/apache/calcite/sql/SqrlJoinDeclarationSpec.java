package org.apache.calcite.sql;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import lombok.Getter;
import org.apache.calcite.sql.parser.SqlParserPos;

@Getter
public class SqrlJoinDeclarationSpec extends SqlCall {
  public static final SqlSpecialOperator OPERATOR = new Operator() {
    public SqlCall createCall(SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
      return new SqrlJoinDeclarationSpec(pos, (SqrlJoinTerm) operands[0],
          Optional.ofNullable((SqlNodeList)operands[1]),
          (Optional)Optional.ofNullable(operands[2]),
          (Optional)Optional.ofNullable(operands[3]),
          (Optional)Optional.ofNullable(operands[4]));
    }
  };

  public final SqrlJoinTerm relation;
  public final Optional<SqlNodeList> orderList;
  public final Optional<SqlNumericLiteral> fetch;
  public final Optional<SqlIdentifier> inverse;
  public final Optional<SqlNodeList> leftJoins; //unbound joins

  public SqrlJoinDeclarationSpec(SqlParserPos pos, SqrlJoinTerm relation,
      Optional<SqlNodeList> orderList,
      Optional<SqlNumericLiteral> fetch, Optional<SqlIdentifier> inverse, Optional<SqlNodeList> leftJoins) {
    super(pos);
    this.relation = relation;
    this.orderList = orderList;
    this.fetch = fetch;
    this.inverse = inverse;
    this.leftJoins = leftJoins;
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public SqlKind getKind() {
    return SqlKind.JOIN_DECLARATION;
  }

  @Override
  public List<SqlNode> getOperandList() {
    SqlNode[] list = {relation, orderList.orElse(null), fetch.orElse(null), inverse.orElse(null),
      leftJoins.orElse(null)};
    return Arrays.asList(list);
  }

  private static class Operator extends SqlSpecialOperator {
    private Operator() {
      super("JOIN DECLARATION SPEC", SqlKind.OTHER, 0);
    }

    public SqlSyntax getSyntax() {
      return SqlSyntax.POSTFIX;
    }

    public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
      //todo: unparse
    }
  }

  public <R, C> R accept(SqrlJoinDeclarationVisitor<R, C> visitor, C context) {
    return visitor.visitJoinDeclaration(this, context);
  }

  public interface SqrlJoinDeclarationVisitor<R, C> {
    R visitJoinDeclaration(SqrlJoinDeclarationSpec node, C context);
  }
}
