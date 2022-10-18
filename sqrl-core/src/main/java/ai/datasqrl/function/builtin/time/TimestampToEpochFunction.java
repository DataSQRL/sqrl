package ai.datasqrl.function.builtin.time;

import ai.datasqrl.function.calcite.FirstArgNullPreservingInference;
import java.time.Instant;
import java.util.List;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;

public class TimestampToEpochFunction extends SqrlScalarFunction {

  static final ScalarFunction fnc = ScalarFunctionImpl.create(Types.lookupMethod(
      StdTimeLibraryImpl.TIMESTAMP_TO_EPOCH.class, "eval", Instant.class));

  public TimestampToEpochFunction() {
    super(
        new SqlIdentifier("TIMESTAMP_TO_EPOCH", SqlParserPos.ZERO),
        SqlKind.OTHER,
        new FirstArgNullPreservingInference(
            RelDataTypeImpl.proto(SqlTypeName.BIGINT, false)),
        InferTypes.ANY_NULLABLE,
        OperandTypes.operandMetadata(
            List.of(SqlTypeFamily.TIMESTAMP),
            typeFactory -> List.of(
                typeFactory.createSqlType(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, 3)),
            i -> "arg" + i,
            i -> false),
        fnc,
        SqlFunctionCategory.USER_DEFINED_FUNCTION
    );
  }
}
