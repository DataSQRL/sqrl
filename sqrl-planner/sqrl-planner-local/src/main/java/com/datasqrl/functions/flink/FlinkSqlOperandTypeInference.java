package com.datasqrl.functions.flink;

import lombok.AllArgsConstructor;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.functions.inference.CallBindingCallContext;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.logical.LogicalType;

import java.util.List;

import static org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTypeFactory;
import static org.apache.flink.table.types.inference.TypeInferenceUtil.createUnexpectedException;

@AllArgsConstructor
public class FlinkSqlOperandTypeInference implements SqlOperandTypeInference {
  private final DataTypeFactory dataTypeFactory;

  private final FunctionDefinition definition;

  private final TypeInference typeInference;

  @Override
  public void inferOperandTypes(SqlCallBinding callBinding, RelDataType returnType, RelDataType[] operandTypes) {
    //todo Planner class
    final CallContext callContext =
        new CallBindingCallContext(dataTypeFactory, definition, callBinding, returnType);
    try {
      inferOperandTypesOrError(unwrapTypeFactory(callBinding), callContext, operandTypes);
    } catch (ValidationException | CalciteContextException e) {
      // let operand checker fail
    } catch (Throwable t) {
      throw createUnexpectedException(callContext, t);
    }
  }

  private void inferOperandTypesOrError(
      FlinkTypeFactory typeFactory, CallContext callContext, RelDataType[] operandTypes) {
    final List<DataType> expectedDataTypes;
    // typed arguments have highest priority
    if (typeInference.getTypedArguments().isPresent()) {
      expectedDataTypes = typeInference.getTypedArguments().get();
    } else {
      expectedDataTypes =
          typeInference
              .getInputTypeStrategy()
              .inferInputTypes(callContext, false)
              .orElse(null);
    }

    // early out for invalid input
    if (expectedDataTypes == null || expectedDataTypes.size() != operandTypes.length) {
      return;
    }

    for (int i = 0; i < operandTypes.length; i++) {
      final LogicalType inferredType = expectedDataTypes.get(i).getLogicalType();
      operandTypes[i] = typeFactory.createFieldTypeFromLogicalType(inferredType);
    }
  }
}
