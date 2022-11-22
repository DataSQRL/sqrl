package ai.datasqrl.function.builtin.example;

import ai.datasqrl.function.SqrlFunction;
import java.util.Optional;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.inference.TypeInference;

public class MyFunction extends ScalarFunction implements SqrlFunction {

  public Long eval(Long value) {
    return value * -1;
  }

  @Override
  public TypeInference getTypeInference(DataTypeFactory typeFactory) {
    return TypeInference.newBuilder()
        .typedArguments(DataTypes.BIGINT())
        .outputTypeStrategy(callContext -> Optional.of(DataTypes.BIGINT()))
        .build();
  }
}