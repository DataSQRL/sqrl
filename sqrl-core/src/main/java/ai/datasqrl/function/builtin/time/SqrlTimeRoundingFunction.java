package ai.datasqrl.function.builtin.time;

import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.*;

import java.util.List;

public class SqrlTimeRoundingFunction extends SqrlScalarFunction {

  public SqrlTimeRoundingFunction(String sqlIdentifier, ScalarFunction scalarFunction) {
    super(
        new SqlIdentifier(sqlIdentifier, SqlParserPos.ZERO),
        SqlKind.OTHER,
        ReturnTypes.TIMESTAMP,
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

  @Override
  public boolean isTimestampPreserving() {
    return true;
  }

  @Override
  public boolean isTimeBucketingFunction() {
    return true;
  }


}