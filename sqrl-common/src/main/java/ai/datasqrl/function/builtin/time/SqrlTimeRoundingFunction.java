package ai.datasqrl.function.builtin.time;

import ai.datasqrl.function.SqrlTimeTumbleFunction;
import ai.datasqrl.function.calcite.FirstArgNullPreservingInference;
import com.google.common.base.Preconditions;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.*;

import java.time.temporal.ChronoUnit;
import java.util.List;

public class SqrlTimeRoundingFunction extends SqrlScalarFunction implements SqrlTimeTumbleFunction {

  private final ChronoUnit timeUnit;

  public SqrlTimeRoundingFunction(String sqlIdentifier, ScalarFunction scalarFunction,
      ChronoUnit timeUnit) {
    super(
        new SqlIdentifier(sqlIdentifier, SqlParserPos.ZERO),
        SqlKind.OTHER,
        new FirstArgNullPreservingInference(
            RelDataTypeImpl.proto(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
                3, false)),
        InferTypes.RETURN_TYPE,
        OperandTypes.operandMetadata(
            List.of(SqlTypeFamily.TIMESTAMP),
            typeFactory -> List.of(
                typeFactory.createSqlType(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, 3)),
            i -> "arg" + i,
            i -> false),
        scalarFunction,
        SqlFunctionCategory.USER_DEFINED_FUNCTION
    );
    this.timeUnit = timeUnit;
  }

  @Override
  public boolean isTimestampPreserving() {
    return true;
  }

  @Override
  public Specification getSpecification(long[] arguments) {
    Preconditions.checkArgument(arguments.length == 0);
    return new Specification();
  }

  private class Specification implements SqrlTimeTumbleFunction.Specification {

    @Override
    public long getBucketWidthMillis() {
      return timeUnit.getDuration().toMillis();
    }
  }

}