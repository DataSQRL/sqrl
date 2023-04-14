package org.apache.calcite.sql;

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Optional;
import lombok.Getter;
import org.apache.calcite.sql.parser.SqlParserPos;

@Getter
public class SqrlJoinPath extends SqrlJoinTerm {
  public static final SqlSpecialOperator OPERATOR = new Operator() {
    public SqlCall createCall(SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
      return new SqrlJoinDeclarationSpec(pos, (SqrlJoinTerm) operands[0],
          Optional.ofNullable((SqlNodeList) operands[1]),
          (Optional) Optional.ofNullable(operands[2]),
          (Optional) Optional.ofNullable(operands[3]),
          (Optional) Optional.ofNullable(operands[4]));
    }
  };

  public final List<SqlNode> relations;
  public final List<SqlNode> conditions;

  public SqrlJoinPath(SqlParserPos pos, List<SqlNode> relations,
      List<SqlNode> conditions) {
    super(pos);
    Preconditions.checkState(relations.size() == conditions.size());
    this.relations = relations;
    this.conditions = conditions;
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return List.of();
  }

  public <R, C> R accept(SqrlJoinTermVisitor<R, C> visitor, C context) {
    return visitor.visitJoinPath(this, context);
  }

  private static class Operator extends SqlSpecialOperator {

    private Operator() {
      super("JOIN PATH", SqlKind.OTHER, 0);
    }

    public SqlSyntax getSyntax() {
      return SqlSyntax.POSTFIX;
    }

    public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
      SqrlJoinPath joinPath = (SqrlJoinPath) call;
      List<SqlNode> relations = joinPath.getRelations();
      List<SqlNode> conditions = joinPath.getConditions();

      // Preconditions check
      Preconditions.checkArgument(
          relations.size() == conditions.size(),
          "Number of relations and conditions must be equal"
      );

      writer.keyword("JOIN PATH");
      for (int i = 0; i < relations.size(); i++) {
        SqlNode relation = relations.get(i);
        SqlNode condition = conditions.get(i);

        writer.keyword("ON");
        writer.sep("(");
        relation.unparse(writer, leftPrec, rightPrec);
        writer.sep(")");
        if (condition != null) {
          writer.keyword("USING");
          condition.unparse(writer, leftPrec, rightPrec);
        }
      }
    }
  }
}
