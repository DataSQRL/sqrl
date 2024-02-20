package com.datasqrl.json;

import static com.datasqrl.json.JsonFunctions.createJsonArgumentTypeStrategy;
import static com.datasqrl.json.JsonFunctions.createJsonType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.inference.InputTypeStrategies;
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategies;

/**
 * Creates a JSON array from the list of JSON objects and scalar values.
 */
public class JsonArray extends ScalarFunction {

  public FlinkJsonType eval(Object... objects) {
    ObjectMapper mapper = new ObjectMapper();
    ArrayNode arrayNode = mapper.createArrayNode();

    for (Object value : objects) {
      if (value instanceof FlinkJsonType) {
        FlinkJsonType type = (FlinkJsonType) value;
        try {
          arrayNode.add(mapper.readTree(type.json));
        } catch (JsonProcessingException e) {
          throw new RuntimeException(e);
        }
      } else {
        arrayNode.addPOJO(value);
      }
    }

    return new FlinkJsonType(arrayNode.toString());
  }

  @Override
  public TypeInference getTypeInference(DataTypeFactory typeFactory) {
    InputTypeStrategy inputTypeStrategy = InputTypeStrategies.varyingSequence(
        createJsonArgumentTypeStrategy(typeFactory));

    return TypeInference.newBuilder().inputTypeStrategy(inputTypeStrategy)
        .outputTypeStrategy(TypeStrategies.explicit(createJsonType(typeFactory))).build();
  }

}