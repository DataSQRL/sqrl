package com.datasqrl.functions.flink;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.planner.calcite.FlinkCalciteSqlValidator;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.functions.inference.OperatorBindingCallContext;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.logical.LogicalType;

import javax.annotation.Nullable;

import static org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTypeFactory;
import static org.apache.flink.table.types.inference.TypeInferenceUtil.*;

public class FlinkSqlReturnTypeInference implements SqlReturnTypeInference {

  private final DataTypeFactory dataTypeFactory;

  private final FunctionDefinition definition;

  private final TypeInference typeInference;

  public FlinkSqlReturnTypeInference(
      DataTypeFactory dataTypeFactory,
      FunctionDefinition definition,
      TypeInference typeInference) {
    this.dataTypeFactory = dataTypeFactory;
    this.definition = definition;
    this.typeInference = typeInference;
  }

  @Override
  public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
    final CallContext callContext =
        new OperatorBindingCallContext(
            dataTypeFactory,
            definition,
            opBinding,
            extractExpectedOutputType(opBinding));
    try {
      return inferReturnTypeOrError(unwrapTypeFactory(opBinding), callContext);
    } catch (ValidationException e) {
      throw createInvalidCallException(callContext, e);
    } catch (Throwable t) {
      throw createUnexpectedException(callContext, t);
    }
  }

  // --------------------------------------------------------------------------------------------

  private @Nullable RelDataType extractExpectedOutputType(SqlOperatorBinding opBinding) {
    if (opBinding instanceof SqlCallBinding) {
      final SqlCallBinding binding = (SqlCallBinding) opBinding;
      final FlinkCalciteSqlValidator validator =
          (FlinkCalciteSqlValidator) binding.getValidator();
      return validator.getExpectedOutputType(binding.getCall()).orElse(null);
    }
    return null;
  }

  private RelDataType inferReturnTypeOrError(
      FlinkTypeFactory typeFactory, CallContext callContext) {
    final LogicalType inferredType =
        inferOutputType(callContext, typeInference.getOutputTypeStrategy())
            .getLogicalType();
    return typeFactory.createFieldTypeFromLogicalType(inferredType);
  }
}
