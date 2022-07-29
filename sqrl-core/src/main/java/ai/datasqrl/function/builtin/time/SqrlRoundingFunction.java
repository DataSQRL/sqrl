package ai.datasqrl.function.builtin.time;

import java.util.List;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;

public class SqrlRoundingFunction extends SqrlScalarFunction {

  public SqrlRoundingFunction(String sqlIdentifier, ScalarFunction scalarFunction) {
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
}