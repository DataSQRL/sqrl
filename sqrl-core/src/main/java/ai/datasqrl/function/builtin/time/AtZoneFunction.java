package ai.datasqrl.function.builtin.time;

import ai.datasqrl.function.SqrlAwareFunction;
import ai.datasqrl.parse.tree.name.Name;
import java.time.Instant;
import java.util.List;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
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

public class AtZoneFunction extends SqlUserDefinedFunction implements SqrlAwareFunction {

  static final ScalarFunction fnc = ScalarFunctionImpl.create(Types.lookupMethod(
      AtZone.class, "atZone", Instant.class, String.class));

  public AtZoneFunction() {
    super(
        new SqlIdentifier("AT_ZONE", SqlParserPos.ZERO),
        SqlKind.OTHER,
        ReturnTypes.TIMESTAMP,
        InferTypes.ANY_NULLABLE,
        OperandTypes.operandMetadata(
            List.of(SqlTypeFamily.TIMESTAMP, SqlTypeFamily.CHARACTER),
            typeFactory -> List.of(
                typeFactory.createSqlType(SqlTypeName.TIMESTAMP),
                typeFactory.createSqlType(SqlTypeName.VARCHAR)),
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
    return true;
  }

  @Override
  public SqlOperator getOp() {
    return this;
  }
}
