package com.datasqrl.json;

import static com.datasqrl.json.JsonFunctions.createJsonArgumentTypeStrategy;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.inference.InputTypeStrategies;
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategies;

public class JsonObject extends ScalarFunction {

  public FlinkJsonType eval(Object... objects) {
    if (objects.length % 2 != 0) {
      throw new IllegalArgumentException("Arguments should be in key-value pairs");
    }

    ObjectMapper mapper = new ObjectMapper();
    ObjectNode objectNode = mapper.createObjectNode();

    for (int i = 0; i < objects.length; i += 2) {
      if (!(objects[i] instanceof String)) {
        throw new IllegalArgumentException("Key must be a string");
      }
      String key = (String) objects[i];
      Object value = objects[i + 1];
      if (value instanceof FlinkJsonType) {
        FlinkJsonType type = (FlinkJsonType) value;
        try {
          objectNode.put(key, mapper.readTree(type.json));
        } catch (JsonProcessingException e) {
          throw new RuntimeException(e);
        }
      } else {
        objectNode.putPOJO(key, value);
      }
    }

    return new FlinkJsonType(objectNode.toString());
  }


  @Override
  public TypeInference getTypeInference(DataTypeFactory typeFactory) {
    InputTypeStrategy anyJsonCompatibleArg = InputTypeStrategies.repeatingSequence(
        createJsonArgumentTypeStrategy(typeFactory));

    InputTypeStrategy inputTypeStrategy = InputTypeStrategies.compositeSequence()
        .finishWithVarying(anyJsonCompatibleArg);

    return TypeInference.newBuilder().inputTypeStrategy(inputTypeStrategy).outputTypeStrategy(
        TypeStrategies.explicit(DataTypes.of(FlinkJsonType.class).toDataType(typeFactory))).build();
  }
}