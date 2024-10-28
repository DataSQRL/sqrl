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
import org.apache.flink.util.jackson.JacksonMapperFactory;

/**
 * Creates a JSON array from the list of JSON objects and scalar values.
 */
public class JsonArray extends ScalarFunction {
  private static final ObjectMapper mapper = JacksonMapperFactory.createObjectMapper();

  public FlinkJsonType eval(Object... objects) {
    ArrayNode arrayNode = mapper.createArrayNode();

    for (Object value : objects) {
      if (value instanceof FlinkJsonType) {
        FlinkJsonType type = (FlinkJsonType) value;
        arrayNode.add(type.json);
      } else {
        arrayNode.addPOJO(value);
      }
    }

    return new FlinkJsonType(arrayNode);
  }

  @Override
  public TypeInference getTypeInference(DataTypeFactory typeFactory) {
    InputTypeStrategy inputTypeStrategy = InputTypeStrategies.varyingSequence(
        createJsonArgumentTypeStrategy(typeFactory));

    return TypeInference.newBuilder().inputTypeStrategy(inputTypeStrategy)
        .outputTypeStrategy(TypeStrategies.explicit(createJsonType(typeFactory))).build();
  }

}