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
import org.apache.flink.util.jackson.JacksonMapperFactory;

/**
 * Creates a JSON object from key-value pairs, where the key is mapped to a field with the
 * associated value. Key-value pairs are provided as a list of even length, with the first element
 * of each pair being the key and the second being the value. If multiple key-value pairs have the
 * same key, the last pair is added to the JSON object.
 */
public class JsonObject extends ScalarFunction {
  static final ObjectMapper mapper = JacksonMapperFactory.createObjectMapper();

  public FlinkJsonType eval(Object... objects) {
    if (objects.length % 2 != 0) {
      throw new IllegalArgumentException("Arguments should be in key-value pairs");
    }

    ObjectNode objectNode = mapper.createObjectNode();

    for (int i = 0; i < objects.length; i += 2) {
      if (!(objects[i] instanceof String)) {
        throw new IllegalArgumentException("Key must be a string");
      }
      String key = (String) objects[i];
      Object value = objects[i + 1];
      if (value instanceof FlinkJsonType type) {
        objectNode.put(key, type.json);
      } else {
        objectNode.putPOJO(key, value);
      }
    }

    return new FlinkJsonType(objectNode);
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