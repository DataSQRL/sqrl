package ai.datasqrl.function.builtin.time;

import ai.datasqrl.function.SqrlAwareFunction;
import ai.datasqrl.parse.tree.name.Name;
import java.util.List;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;

public class SqrlGettersFunction extends SqlUserDefinedFunction implements SqrlAwareFunction {

  public SqrlGettersFunction(String UnitToGet, ScalarFunction scalarFunction) {
    super(
        new SqlIdentifier("GET_" + UnitToGet, SqlParserPos.ZERO),
        SqlKind.OTHER,
        ReturnTypes.INTEGER,
        InferTypes.ANY_NULLABLE,
        OperandTypes.operandMetadata(
            List.of(SqlTypeFamily.TIMESTAMP),
            typeFactory -> List.of(
                typeFactory.createSqlType(SqlTypeName.TIMESTAMP)),
            i -> "arg" + i,
            i -> false),
        scalarFunction,
        SqlFunctionCategory.USER_DEFINED_FUNCTION
    );
  }

  public SqlSyntax getSyntax() {
    return SqlSyntax.FUNCTION;
  }

  public boolean isDynamicFunction() {
    return true;
  }

  @Override
  public Name getSqrlName() {
    return Name.system(getName());
  }

  @Override
  public boolean isAggregate() {
    return false;
  }

  @Override
  public boolean isTimestampPreserving() {
    return false;
  }

  @Override
  public SqlOperator getOp() {
    return this;
  }
}
