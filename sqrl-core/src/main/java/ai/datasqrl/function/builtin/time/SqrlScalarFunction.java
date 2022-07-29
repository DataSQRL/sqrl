package ai.datasqrl.function.builtin.time;

import ai.datasqrl.function.SqrlAwareFunction;
import ai.datasqrl.parse.tree.name.Name;
import org.apache.calcite.schema.Function;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.type.SqlOperandMetadata;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;

public class SqrlScalarFunction extends SqlUserDefinedFunction implements SqrlAwareFunction {

  public SqrlScalarFunction(SqlIdentifier opName,
      SqlKind kind,
      SqlReturnTypeInference returnTypeInference,
      SqlOperandTypeInference operandTypeInference,
      SqlOperandMetadata operandMetadata,
      Function function) {
    super(opName, kind, returnTypeInference, operandTypeInference, operandMetadata, function);
  }

  protected SqrlScalarFunction(SqlIdentifier opName, SqlKind kind,
      SqlReturnTypeInference returnTypeInference, SqlOperandTypeInference operandTypeInference,
      SqlOperandMetadata operandMetadata, Function function,
      SqlFunctionCategory category) {
    super(opName, kind, returnTypeInference, operandTypeInference, operandMetadata, function,
        category);
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
