package com.datasqrl.secure;

import java.util.Optional;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.inference.TypeInference;

public class Uuid extends ScalarFunction {

  public String eval() {
    return java.util.UUID.randomUUID().toString();
  }

  @Override
  public TypeInference getTypeInference(DataTypeFactory typeFactory) {
    return TypeInference.newBuilder().typedArguments()
        .outputTypeStrategy(callContext -> Optional.of(DataTypes.CHAR(36).notNull())).build();
  }

//    @Override
//    public String getDocumentation() {
//      return "Generates a random UUID string";
//    }
}