package com.datasqrl.plan;

import java.lang.reflect.Type;
import java.util.List;
import lombok.AllArgsConstructor;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.TableFunction;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlOperandMetadata;
import org.apache.calcite.sql.type.SqlOperandTypeChecker.Consistency;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.validate.SqlUserDefinedTableFunction;

public class SqlPlannerTableFunction extends SqlUserDefinedTableFunction {

  public SqlPlannerTableFunction(String name, RelDataType type) {
    super(new SqlIdentifier(name, SqlParserPos.ZERO),
        SqlKind.OTHER_FUNCTION,
        sqlOperatorBinding -> type, null, new OperandMetadata(),
        new PlannerTableFunction(type));
  }

  @AllArgsConstructor
  public static class PlannerTableFunction implements TableFunction {
    RelDataType relDataType;

    @Override
    public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory, List<Object> list) {
      return relDataType;
    }

    @Override
    public Type getElementType(List<Object> list) {
      return Object.class;
    }

    @Override
    public List<FunctionParameter> getParameters() {
      return List.of();
    }
  }

  public static class OperandMetadata implements SqlOperandMetadata {
    @Override
    public List<RelDataType> paramTypes(RelDataTypeFactory relDataTypeFactory) {
      return List.of();
    }

    @Override
    public List<String> paramNames() {
      return List.of();
    }

    @Override
    public boolean checkOperandTypes(SqlCallBinding sqlCallBinding, boolean b) {
      return true;
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
      return new SqlOperandCountRange() {

        @Override
        public boolean isValidCount(int i) {
          return true;
        }

        @Override
        public int getMin() {
          return 0;
        }

        @Override
        public int getMax() {
          return Integer.MAX_VALUE;
        }
      };
    }

    @Override
    public String getAllowedSignatures(SqlOperator sqlOperator, String s) {
      return null;
    }

    @Override
    public Consistency getConsistency() {
      return null;
    }

    @Override
    public boolean isOptional(int i) {
      return true;
    }
  }
}
