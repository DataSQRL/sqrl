package org.apache.calcite.sql;

import java.util.List;
import lombok.Getter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.checkerframework.checker.nullness.qual.Nullable;

@Getter
public class SpecialSqlFunction extends SqlFunction {

  private SqlAggFunction original;

  public SpecialSqlFunction(String name, SqlKind kind,
      @Nullable SqlReturnTypeInference returnTypeInference,
      @Nullable SqlOperandTypeInference operandTypeInference,
      @Nullable SqlOperandTypeChecker operandTypeChecker,
      SqlFunctionCategory category,
      SqlAggFunction original) {
    super(name, kind, returnTypeInference, operandTypeInference, operandTypeChecker, category);
    this.original = original;
  }

  public SpecialSqlFunction(SqlIdentifier sqlIdentifier,
      @Nullable SqlReturnTypeInference returnTypeInference,
      @Nullable SqlOperandTypeInference operandTypeInference,
      @Nullable SqlOperandTypeChecker operandTypeChecker,
      @Nullable List<RelDataType> paramTypes,
      SqlFunctionCategory funcType,
      SqlAggFunction original) {
    super(sqlIdentifier, returnTypeInference, operandTypeInference, operandTypeChecker, paramTypes,
        funcType);
    this.original = original;
  }

  protected SpecialSqlFunction(String name,
      @Nullable SqlIdentifier sqlIdentifier, SqlKind kind,
      @Nullable SqlReturnTypeInference returnTypeInference,
      @Nullable SqlOperandTypeInference operandTypeInference,
      @Nullable SqlOperandTypeChecker operandTypeChecker,
      SqlFunctionCategory category,
      SqlAggFunction original) {
    super(name, sqlIdentifier, kind, returnTypeInference, operandTypeInference, operandTypeChecker,
        category);
    this.original = original;
  }
}
