package ai.datasqrl.function.builtin.time;

import ai.datasqrl.function.calcite.FirstArgNullPreservingInference;
import java.util.List;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;

public class ExtractTimeFieldFunction extends SqrlScalarFunction {

  public ExtractTimeFieldFunction(String sqlIdentifier, ScalarFunction scalarFunction) {
    super(
        new SqlIdentifier(sqlIdentifier, SqlParserPos.ZERO),
        SqlKind.OTHER,
        new FirstArgNullPreservingInference(
            RelDataTypeImpl.proto(SqlTypeName.BIGINT, false)),
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
