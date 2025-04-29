package com.datasqrl.functions;

import com.datasqrl.flinkrunner.functions.AutoRegisterSystemFunction;
import com.google.auto.service.AutoService;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.inference.InputTypeStrategies;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategies;

@AutoService(ScalarFunction.class)
public class TestFunction extends ScalarFunction {

  public boolean eval(Object... objects) {
    return true;
  }

  @Override
  public TypeInference getTypeInference(DataTypeFactory typeFactory) {
    var inputTypeStrategy = InputTypeStrategies.compositeSequence()
        .finishWithVarying(InputTypeStrategies.WILDCARD);

    return TypeInference.newBuilder().inputTypeStrategy(inputTypeStrategy).outputTypeStrategy(
        TypeStrategies.explicit(DataTypes.BOOLEAN())).build();
  }
}
