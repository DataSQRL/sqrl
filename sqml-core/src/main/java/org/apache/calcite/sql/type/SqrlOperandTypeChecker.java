package org.apache.calcite.sql.type;

import static org.apache.calcite.util.Static.RESOURCE;

import ai.dataeng.sqml.parser.Relationship.Multiplicity;
import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;

/**
 * Determines if an operand can support a to-many column
 */
public class SqrlOperandTypeChecker implements SqlOperandTypeChecker {
  private final SqlOperandTypeChecker delegate;

  public SqrlOperandTypeChecker(SqlOperandTypeChecker delegate) {
    this.delegate = delegate;
  }

  private boolean hasPath(SqlCallBinding callBinding, List<SqlNode> operands) {
    for (SqlNode operand : operands) {
      RelDataType type = SqlTypeUtil.deriveType(callBinding, operand);
      if (type instanceof SqrlBasicSqlType) {
        if (((SqrlBasicSqlType)type).getMultiplicity() == Multiplicity.MANY) {
          return true;
        }
      }
    }

    return false;
  }

  /**
   * Check pathable operand
   */
  @Override
  public boolean checkOperandTypes(SqlCallBinding callBinding,
      boolean throwOnFailure) {
    if (hasPath(callBinding,
        callBinding.operands())) {
      if (throwOnFailure) {
        throw callBinding.newError(
            RESOURCE.typeNotSupported("to-many multiplicity for operand(s): " + callBinding.operands()));
      }
      return false;
    }

    return delegate.checkOperandTypes(callBinding, throwOnFailure);
  }

  @Override
  public SqlOperandCountRange getOperandCountRange() {
    return delegate.getOperandCountRange();
  }

  @Override
  public String getAllowedSignatures(SqlOperator op, String opName) {
    return delegate.getAllowedSignatures(op, opName);
  }

  @Override
  public Consistency getConsistency() {
    return delegate.getConsistency();
  }

  @Override
  public boolean isOptional(int i) {
    return delegate.isOptional(i);
  }

  @Override
  public boolean isFixedParameters() {
    return delegate.isFixedParameters();
  }
}
