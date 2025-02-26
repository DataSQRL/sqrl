package com.datasqrl.text;

import com.datasqrl.function.FlinkTypeUtil;
import com.datasqrl.function.FlinkTypeUtil.VariableArguments;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.inference.TypeInference;

/** Replaces the placeholders in the first argument with the remaining arguments in order. */
public class Format extends ScalarFunction {

  public String eval(String text, String... arguments) {
    if (text == null) {
      return null;
    }
    return String.format(text, (Object[]) arguments);
  }

  @Override
  public TypeInference getTypeInference(DataTypeFactory typeFactory) {
    return TypeInference.newBuilder()
        .inputTypeStrategy(
            VariableArguments.builder()
                .staticType(DataTypes.STRING())
                .variableType(DataTypes.STRING())
                .minVariableArguments(0)
                .maxVariableArguments(Integer.MAX_VALUE)
                .build())
        .outputTypeStrategy(FlinkTypeUtil.nullPreservingOutputStrategy(DataTypes.STRING()))
        .build();
  }
}
