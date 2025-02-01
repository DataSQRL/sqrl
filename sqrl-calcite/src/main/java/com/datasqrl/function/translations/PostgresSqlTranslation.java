package com.datasqrl.function.translations;

import static com.datasqrl.function.CalciteFunctionUtil.lightweightAggOp;

import com.datasqrl.calcite.Dialect;
import lombok.Getter;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.flink.table.functions.FunctionDefinition;

@Getter
public abstract class PostgresSqlTranslation extends AbstractDialectSqlTranslation {

  public PostgresSqlTranslation(SqlOperator operator) {
    super(Dialect.POSTGRES, operator);
  }
  public PostgresSqlTranslation(FunctionDefinition operator) {
    super(Dialect.POSTGRES, operator);
  }

  public static class Simple extends PostgresSqlTranslation {

    private final SqlOperator toOperator;

    public Simple(FunctionDefinition fromOperator, SqlOperator toOperator) {
      super(fromOperator);
      this.toOperator = toOperator;
    }

    public Simple(FunctionDefinition fromOperator, String toOperatorName) {
      this(fromOperator, lightweightAggOp(toOperatorName));
    }

    @Override
    public void unparse(SqlCall call, SqlWriter writer, int leftPrec, int rightPrec) {
      toOperator.createCall(SqlParserPos.ZERO, call.getOperandList())
          .unparse(writer, leftPrec, rightPrec);
    }

  }
}
