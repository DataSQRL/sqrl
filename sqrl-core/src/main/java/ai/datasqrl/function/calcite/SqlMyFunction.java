package ai.datasqrl.function.calcite;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlSingleOperandTypeChecker;

public class SqlMyFunction extends SqlFunction {
  public SqlMyFunction() {
    //Must be uppercase
    super("MY_FUNCTION", SqlKind.OTHER_FUNCTION,
        ReturnTypes.INTEGER, (SqlOperandTypeInference)null,
        OperandTypes.or(new SqlSingleOperandTypeChecker[]{OperandTypes.NUMERIC, OperandTypes.NILADIC}),
        SqlFunctionCategory.NUMERIC);
  }

  public SqlSyntax getSyntax() {
    return SqlSyntax.FUNCTION;
  }

  public boolean isDynamicFunction() {
    return true;
  }
}