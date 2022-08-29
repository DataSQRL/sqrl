package ai.datasqrl.function.builtin.time;

import java.util.List;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;

public class NumToTimestampFunction extends SqrlScalarFunction {

  static final ScalarFunction fnc = ScalarFunctionImpl.create(Types.lookupMethod(
      StdTimeLibraryImpl.class, "numToTimestamp", Long.class));

  public NumToTimestampFunction() {
    super(
        new SqlIdentifier("NUM_TO_TIMESTAMP", SqlParserPos.ZERO),
        SqlKind.OTHER,
        ReturnTypes.explicit(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, 3),
        //ReturnTypes.explicit(SqlTypeName.TIMESTAMP),
        InferTypes.ANY_NULLABLE,
        OperandTypes.operandMetadata(
            List.of(SqlTypeFamily.INTEGER),
            typeFactory -> List.of(
                typeFactory.createSqlType(SqlTypeName.INTEGER),
                typeFactory.createSqlType(SqlTypeName.BIGINT)),
            i -> "arg" + i,
            i -> false),
        fnc,
        SqlFunctionCategory.USER_DEFINED_FUNCTION
    );
  }
}

