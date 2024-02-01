package com.datasqrl.json;

import com.datasqrl.function.SqrlFunction;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.ReadContext;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.SneakyThrows;
import lombok.Value;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.runtime.functions.SqlJsonUtils;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.ArgumentTypeStrategy;
import org.apache.flink.table.types.inference.InputTypeStrategies;
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategies;
import org.apache.flink.table.types.inference.strategies.SpecificInputTypeStrategies;

public class JsonFunctions {

  public static final ToJson TO_JSON = new ToJson();
  public static final JsonToString JSON_TO_STRING = new JsonToString();
  public static final JsonObject JSON_OBJECT = new JsonObject();
  public static final JsonArray JSON_ARRAY = new JsonArray();
  public static final JsonExtract JSON_EXTRACT = new JsonExtract();
  public static final JsonQuery JSON_QUERY = new JsonQuery();
  public static final JsonExists JSON_EXISTS = new JsonExists();
  public static final JsonArrayAgg JSON_ARRAYAGG = new JsonArrayAgg();
  public static final JsonObjectAgg JSON_OBJECTAGG = new JsonObjectAgg();
  public static final JsonConcat JSON_CONCAT = new JsonConcat();

  public static final ObjectMapper mapper = new ObjectMapper();

  public static ArgumentTypeStrategy createJsonArgumentTypeStrategy(DataTypeFactory typeFactory) {
    return InputTypeStrategies.or(SpecificInputTypeStrategies.JSON_ARGUMENT,
        InputTypeStrategies.explicit(createJsonType(typeFactory)));
  }

  public static DataType createJsonType(DataTypeFactory typeFactory) {
    DataType dataType = DataTypes.of(FlinkJsonType.class).toDataType(typeFactory);
    return dataType;
  }

  public static class ToJson extends ScalarFunction implements SqrlFunction {

    public FlinkJsonType eval(String json) {
      if (json == null) {
        return null;
      }
      try {
        return new FlinkJsonType(mapper.readTree(json).toString());
      } catch (JsonProcessingException e) {
        return null;
      }
    }

    @Override
    public String getDocumentation() {
      return "Parses a JSON object from string";
    }
  }

  public static class JsonToString extends ScalarFunction implements SqrlFunction {

    public String eval(FlinkJsonType json) {
      if (json == null) {
        return null;
      }
      return json.getJson();
    }

    @Override
    public String getDocumentation() {
      return "Converts a JSON object to string";
    }
  }


  public static class JsonObject extends ScalarFunction implements SqrlFunction {

    @SneakyThrows
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
          objectNode.putIfAbsent(key, mapper.readTree(type.json));
        } else {
          objectNode.putPOJO(key, value);
        }
      }

      return new FlinkJsonType(objectNode.toString());
    }


    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      InputTypeStrategy anyJsonCompatibleArg =
          InputTypeStrategies.repeatingSequence(createJsonArgumentTypeStrategy(typeFactory));

      InputTypeStrategy inputTypeStrategy = InputTypeStrategies
          .compositeSequence()
          .finishWithVarying(anyJsonCompatibleArg);

      return TypeInference.newBuilder()
          .inputTypeStrategy(inputTypeStrategy)
          .outputTypeStrategy(
              TypeStrategies.explicit(DataTypes.of(FlinkJsonType.class).toDataType(typeFactory)))
          .build();
    }

    @Override
    public String getDocumentation() {
      return "Creates a JSON object from key-value pairs, where the key is mapped to a field with the associated value."
          + " Key-value pairs are provided as a list of even length, with the first element of each pair being the key and the second being the value."
          + " If multiple key-value pairs have the same key, the last pair is added to the JSON object.";
    }
  }

  public static class JsonArray extends ScalarFunction implements SqrlFunction {

    @SneakyThrows
    public FlinkJsonType eval(Object... objects) {
      ObjectMapper mapper = new ObjectMapper();
      ArrayNode arrayNode = mapper.createArrayNode();

      for (Object value : objects) {
        if (value instanceof FlinkJsonType) {
          FlinkJsonType type = (FlinkJsonType) value;
          arrayNode.add(mapper.readTree(type.json));
        } else {
          arrayNode.addPOJO(value); // putPOJO to handle arbitrary objects
        }
      }

      return new FlinkJsonType(arrayNode.toString());
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      InputTypeStrategy inputTypeStrategy = InputTypeStrategies.varyingSequence(
          createJsonArgumentTypeStrategy(typeFactory));

      return TypeInference.newBuilder()
          .inputTypeStrategy(inputTypeStrategy)
          .outputTypeStrategy(TypeStrategies.explicit(createJsonType(typeFactory)))
          .build();
    }

    @Override
    public String getDocumentation() {
      return "Creates a JSON array from the list of JSON objects and scalar values.";
    }
  }

  public static class JsonExtract extends ScalarFunction implements SqrlFunction {

    public String eval(FlinkJsonType input, String pathSpec) {
      if (input == null) {
        return null;
      }
      try {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode jsonNode = mapper.readTree(input.getJson());
        ReadContext ctx = JsonPath.parse(jsonNode.toString());
        return ctx.read(pathSpec);
      } catch (Exception e) {
        return null;
      }
    }

    public String eval(FlinkJsonType input, String pathSpec, String defaultValue) {
      if (input == null) {
        return null;
      }
      try {
        ReadContext ctx = JsonPath.parse(input.getJson());
        JsonPath parse = JsonPath.compile(pathSpec);
        return ctx.read(parse, String.class);
      } catch (Exception e) {
        return defaultValue;
      }
    }

    public Boolean eval(FlinkJsonType input, String pathSpec, boolean defaultValue) {
      if (input == null) {
        return null;
      }
      try {
        ReadContext ctx = JsonPath.parse(input.getJson());
        JsonPath parse = JsonPath.compile(pathSpec);
        return ctx.read(parse, Boolean.class);
      } catch (Exception e) {
        return defaultValue;
      }
    }

    public Double eval(FlinkJsonType input, String pathSpec, Double defaultValue) {
      if (input == null) {
        return null;
      }
      try {
        ReadContext ctx = JsonPath.parse(input.getJson());
        JsonPath parse = JsonPath.compile(pathSpec);
        return ctx.read(parse, Double.class);
      } catch (Exception e) {
        return defaultValue;
      }
    }

    public Integer eval(FlinkJsonType input, String pathSpec, Integer defaultValue) {
      if (input == null) {
        return null;
      }
      try {
        ReadContext ctx = JsonPath.parse(input.getJson());
        JsonPath parse = JsonPath.compile(pathSpec);
        return ctx.read(parse, Integer.class);
      } catch (Exception e) {
        return defaultValue;
      }
    }

    @Override
    public String getDocumentation() {
      return "Extracts a value from the JSON object based on the provided JSON path. An optional third "
          + "argument can be provided to specify a default value when the given JSON path does not yield "
          + "a value for the JSON object.";
    }
  }

  public static class JsonQuery extends ScalarFunction implements SqrlFunction {

    public String eval(FlinkJsonType input, String pathSpec) {
      if (input == null) {
        return null;
      }
      try {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode jsonNode = mapper.readTree(input.getJson());
        ReadContext ctx = JsonPath.parse(jsonNode.toString());
        Object result = ctx.read(pathSpec);
        return mapper.writeValueAsString(result); // Convert the result back to JSON string
      } catch (Exception e) {
        return null;
      }
    }

    @Override
    public String getDocumentation() {
      return "For a given JSON object, executes a JSON path query against the object and returns the result as string.";
    }
  }

  public static class JsonExists extends ScalarFunction implements SqrlFunction {

    public Boolean eval(FlinkJsonType json, String path) {
      if (json == null) {
        return null;
      }
      try {
        return SqlJsonUtils.jsonExists(json.json, path);
      } catch (Exception e) {
        return false;
      }
    }

    @Override
    public String getDocumentation() {
      return "For a given JSON object, checks whether the provided JSON path exists";
    }
  }

  public static class JsonConcat extends ScalarFunction implements SqrlFunction {

    private final ObjectMapper mapper = new ObjectMapper();

    public FlinkJsonType eval(FlinkJsonType json1, FlinkJsonType json2) {
      if (json1 == null || json2 == null) {
        return null;
      }
      try {
        ObjectNode node1 = (ObjectNode) mapper.readTree(json1.getJson());
        ObjectNode node2 = (ObjectNode) mapper.readTree(json2.getJson());

        node1.setAll(node2);
        return new FlinkJsonType(node1.toString());
      } catch (Exception e) {
        return null;
      }
    }

    @Override
    public String getDocumentation() {
      return "Merges two JSON objects into one. If two objects share the same key, the value from the later object is used.";
    }
  }

  @Value
  public static class ArrayAgg {

    @DataTypeHint(value = "RAW")
    List<Object> objects;

    public void add(Object value) {
      objects.add(value);
    }

    public void remove(Object value) {
      objects.remove(value);
    }
  }

  public static class JsonArrayAgg extends AggregateFunction<FlinkJsonType, ArrayAgg> implements
      SqrlFunction {

    private final ObjectMapper mapper = new ObjectMapper();


    @Override
    public ArrayAgg createAccumulator() {
      return new ArrayAgg(new ArrayList<>());
    }

    public void accumulate(ArrayAgg accumulator, String value) {
      accumulator.add(value);
    }

    @SneakyThrows
    public void accumulate(ArrayAgg accumulator, FlinkJsonType value) {
      if (value != null) {
        accumulator.add(mapper.readTree(value.json));
      } else {
        accumulator.add(null);
      }
    }

    public void accumulate(ArrayAgg accumulator, Double value) {
      accumulator.add(value);
    }

    public void accumulate(ArrayAgg accumulator, Long value) {
      accumulator.add(value);
    }

    public void accumulate(ArrayAgg accumulator, Integer value) {
      accumulator.add(value);
    }

    public void retract(ArrayAgg accumulator, String value) {
      accumulator.remove(value);
    }

    @SneakyThrows
    public void retract(ArrayAgg accumulator, FlinkJsonType value) {
      if (value != null) {
        JsonNode jsonNode = mapper.readTree(value.json);
        accumulator.remove(jsonNode);
      } else {
        accumulator.remove(null);
      }
    }

    public void retract(ArrayAgg accumulator, Double value) {
      accumulator.remove(value);
    }

    public void retract(ArrayAgg accumulator, Long value) {
      accumulator.remove(value);
    }

    public void retract(ArrayAgg accumulator, Integer value) {
      accumulator.remove(value);
    }

    @Override
    public FlinkJsonType getValue(ArrayAgg accumulator) {
      ArrayNode arrayNode = mapper.createArrayNode();
      for (Object o : accumulator.getObjects()) {
        if (o instanceof FlinkJsonType) {
          try {
            arrayNode.add(mapper.readTree(((FlinkJsonType) o).json));
          } catch (JsonProcessingException e) {
            return null;
          }
        } else {
          arrayNode.addPOJO(o);
        }
      }
      return new FlinkJsonType(arrayNode.toString());
    }

    @Override
    public String getDocumentation() {
      return "Aggregation function that aggregates JSON objects into a JSON array.";
    }
  }

  @Value
  public static class ObjectAgg {

    @DataTypeHint(value = "RAW")
    Map<String, Object> objects;

    public void add(String key, Object value) {
      if (key != null) {
        objects.put(key, value);
      }
    }

    public void remove(String key) {
      if (key != null) {
        objects.remove(key);
      }
    }
  }

  public static class JsonObjectAgg extends
      AggregateFunction<FlinkJsonType, ObjectAgg> implements SqrlFunction {

    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public ObjectAgg createAccumulator() {
      return new ObjectAgg(new HashMap<>());
    }

    public void accumulate(ObjectAgg accumulator, String key, String value) {
      accumulateObject(accumulator, key, value);
    }

    @SneakyThrows
    public void accumulate(ObjectAgg accumulator, String key, FlinkJsonType value) {
      if (value != null) {
        accumulateObject(accumulator, key, mapper.readTree(value.getJson()));
      } else {
        accumulator.add(key, null);
      }
    }

    public void accumulate(ObjectAgg accumulator, String key, Double value) {
      accumulateObject(accumulator, key, value);
    }

    public void accumulate(ObjectAgg accumulator, String key, Long value) {
      accumulateObject(accumulator, key, value);
    }

    public void accumulate(ObjectAgg accumulator, String key, Integer value) {
      accumulateObject(accumulator, key, value);
    }

    public void accumulateObject(ObjectAgg accumulator, String key, Object value) {
      accumulator.add(key, value);
    }

    public void retract(ObjectAgg accumulator, String key, String value) {
      retractObject(accumulator, key);
    }

    public void retract(ObjectAgg accumulator, String key, FlinkJsonType value) {
      retractObject(accumulator, key);
    }

    public void retract(ObjectAgg accumulator, String key, Double value) {
      retractObject(accumulator, key);
    }

    public void retract(ObjectAgg accumulator, String key, Long value) {
      retractObject(accumulator, key);
    }

    public void retract(ObjectAgg accumulator, String key, Integer value) {
      retractObject(accumulator, key);
    }

    public void retractObject(ObjectAgg accumulator, String key) {
      accumulator.remove(key);
    }

    @Override
    public FlinkJsonType getValue(ObjectAgg accumulator) {
      ObjectNode objectNode = mapper.createObjectNode();
      accumulator.getObjects().forEach((key, value) -> {
        if (value instanceof FlinkJsonType) {
          try {
            objectNode.set(key, mapper.readTree(((FlinkJsonType) value).json));
          } catch (JsonProcessingException e) {
            // Ignore value
          }
        } else {
          objectNode.putPOJO(key, value);
        }
      });
      return new FlinkJsonType(objectNode.toString());
    }

    @Override
    public String getDocumentation() {
      return "Aggregation function that merges JSON objects into a single JSON object. If two JSON"
          + "objects share the same field name, the value of the later one is used in the aggregated result.";
    }
  }
}