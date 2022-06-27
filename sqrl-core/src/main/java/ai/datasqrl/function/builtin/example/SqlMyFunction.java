package ai.datasqrl.function.builtin.example;

import java.util.List;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;

public class SqlMyFunction extends SqlUserDefinedFunction {
  static final ScalarFunction fnc =
      ScalarFunctionImpl.create(Types.lookupMethod(MyFunction.class, "eval", Long.class));

  public SqlMyFunction() {
    super(
        new SqlIdentifier("MY_FUNCTION", SqlParserPos.ZERO),
        SqlKind.OTHER,
        ReturnTypes.BIGINT,
        InferTypes.RETURN_TYPE,
        OperandTypes.operandMetadata(
            List.of(SqlTypeFamily.NUMERIC),
            typeFactory -> List.of(
                typeFactory.createSqlType(SqlTypeName.BIGINT),
                typeFactory.createSqlType(SqlTypeName.INTEGER)),
            i -> "arg" + i,
            i -> false),
        fnc,
        SqlFunctionCategory.USER_DEFINED_FUNCTION
    );
  }

  public SqlSyntax getSyntax() {
    return SqlSyntax.FUNCTION;
  }

  public boolean isDynamicFunction() {
    return true;
  }
}