package com.datasqrl.secure;

import java.util.Optional;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.inference.TypeInference;

import com.datasqrl.function.StandardLibraryFunction;
import com.google.auto.service.AutoService;

/**
 * Generates a random UUID string
 */
@AutoService(StandardLibraryFunction.class)
public class Uuid extends ScalarFunction implements StandardLibraryFunction {

  public String eval() {
    return java.util.UUID.randomUUID().toString();
  }

  @Override
  public TypeInference getTypeInference(DataTypeFactory typeFactory) {
    return TypeInference.newBuilder().typedArguments()
        .outputTypeStrategy(callContext -> Optional.of(DataTypes.CHAR(36).notNull())).build();
  }

  @Override
  public boolean isDeterministic() {
    return false;
  }
}