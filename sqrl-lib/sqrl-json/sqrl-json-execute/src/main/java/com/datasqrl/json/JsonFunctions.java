package com.datasqrl.json;

import com.datasqrl.function.SqrlFunction;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.ReadContext;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.SneakyThrows;
import lombok.Value;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.annotation.InputGroup;
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
    DataType rawDataType = typeFactory.createRawDataType(JsonNode.class);
    return rawDataType;
  }

  @FunctionHint(output = @DataTypeHint(value = "RAW", bridgedTo = JsonNode.class))
  public static class ToJson extends ScalarFunction implements SqrlFunction {

    //todo: Add other types here
    public JsonNode eval(String json) {
      if (json == null) {
        return null;
      }
      try {
        return mapper.readTree(json);
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

    @FunctionHint(input = @DataTypeHint(value = "RAW", bridgedTo = JsonNode.class))

    public String eval(JsonNode json) {
      if (json == null) {
        return null;
      }
      return json.toString();
    }

    @Override
    public String getDocumentation() {
      return "Converts a JSON object to string";
    }
  }


  @FunctionHint(output = @DataTypeHint(value = "RAW", bridgedTo = JsonNode.class))

  public static class JsonObject extends ScalarFunction implements SqrlFunction {

    @SneakyThrows
    public JsonNode eval(Object... objects) {
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
        if (value instanceof JsonNode) {
          JsonNode type = (JsonNode) value;
          objectNode.replace(key, type);
        } else {
          objectNode.putPOJO(key, value);
        }
      }

      return objectNode;
    }


    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      InputTypeStrategy anyJsonCompatibleArg = InputTypeStrategies.repeatingSequence(
          createJsonArgumentTypeStrategy(typeFactory));

      InputTypeStrategy inputTypeStrategy = InputTypeStrategies.compositeSequence()
          .finishWithVarying(anyJsonCompatibleArg);

      return TypeInference.newBuilder().inputTypeStrategy(inputTypeStrategy).outputTypeStrategy(
          TypeStrategies.explicit(typeFactory.createRawDataType(JsonNode.class))).build();
    }

    @Override
    public String getDocumentation() {
      return
          "Creates a JSON object from key-value pairs, where the key is mapped to a field with the associated value."
              + " Key-value pairs are provided as a list of even length, with the first element of each pair being the key and the second being the value."
              + " If multiple key-value pairs have the same key, the last pair is added to the JSON object.";
    }
  }

  @FunctionHint(output = @DataTypeHint(value = "RAW", bridgedTo = JsonNode.class))

  public static class JsonArray extends ScalarFunction implements SqrlFunction {

    @SneakyThrows
    public JsonNode eval(Object... objects) {
      ObjectMapper mapper = new ObjectMapper();
      ArrayNode arrayNode = mapper.createArrayNode();

      for (Object value : objects) {
        if (value instanceof JsonNode) {
          JsonNode type = (JsonNode) value;
          arrayNode.add(type);
        } else {
          arrayNode.addPOJO(value); // putPOJO to handle arbitrary objects
        }
      }

      return arrayNode;
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
      InputTypeStrategy inputTypeStrategy = InputTypeStrategies.varyingSequence(
          createJsonArgumentTypeStrategy(typeFactory));

      return TypeInference.newBuilder().inputTypeStrategy(inputTypeStrategy)
          .outputTypeStrategy(TypeStrategies.explicit(createJsonType(typeFactory))).build();
    }

    @Override
    public String getDocumentation() {
      return "Creates a JSON array from the list of JSON objects and scalar values.";
    }
  }

  public static class JsonExtract extends ScalarFunction implements SqrlFunction {

    @SneakyThrows
    @FunctionHint(input = {@DataTypeHint(value = "RAW", bridgedTo = JsonNode.class),
        @DataTypeHint(bridgedTo = String.class)})
    public String eval(JsonNode input, String pathSpec) {
      if (input == null) {
        return null;
      }
      try {
        ReadContext ctx = JsonPath.parse(input.toString());
        return ctx.read(pathSpec);
      } catch (Exception e) {
        e.printStackTrace();
        return null;
      }
    }

    //Todo, migrate to better type inference strategy
    @FunctionHint(input = {@DataTypeHint(value = "RAW", bridgedTo = JsonNode.class),
        @DataTypeHint(bridgedTo = String.class), @DataTypeHint(bridgedTo = String.class),})
    public String eval(JsonNode input, String pathSpec, String defaultValue) {
      if (input == null) {
        return null;
      }
      try {
        ReadContext ctx = JsonPath.parse(input.toString());
        JsonPath parse = JsonPath.compile(pathSpec);
        return ctx.read(parse, String.class);
      } catch (Exception e) {
        e.printStackTrace();
        return defaultValue;
      }
    }

    @FunctionHint(input = {@DataTypeHint(value = "RAW", bridgedTo = JsonNode.class),
        @DataTypeHint(bridgedTo = String.class), @DataTypeHint(bridgedTo = Boolean.class),})
    public Boolean eval(JsonNode input, String pathSpec, Boolean defaultValue) {
      if (input == null) {
        return null;
      }
      try {
        ReadContext ctx = JsonPath.parse(input.toString());
        JsonPath parse = JsonPath.compile(pathSpec);
        return ctx.read(parse, Boolean.class);
      } catch (Exception e) {
        return defaultValue;
      }
    }

    @FunctionHint(input = {@DataTypeHint(value = "RAW", bridgedTo = JsonNode.class),
        @DataTypeHint(bridgedTo = String.class), @DataTypeHint(bridgedTo = Double.class),})
    public Double eval(JsonNode input, String pathSpec, Double defaultValue) {
      if (input == null) {
        return null;
      }
      try {
        ReadContext ctx = JsonPath.parse(input.toString());
        JsonPath parse = JsonPath.compile(pathSpec);
        return ctx.read(parse, Double.class);
      } catch (Exception e) {
        return defaultValue;
      }
    }

    @FunctionHint(input = {@DataTypeHint(value = "RAW", bridgedTo = JsonNode.class),
        @DataTypeHint(bridgedTo = String.class), @DataTypeHint(bridgedTo = Integer.class),})
    public Integer eval(JsonNode input, String pathSpec, Integer defaultValue) {
      if (input == null) {
        return null;
      }
      try {
        ReadContext ctx = JsonPath.parse(input.toString());
        JsonPath parse = JsonPath.compile(pathSpec);
        return ctx.read(parse, Integer.class);
      } catch (Exception e) {
        return defaultValue;
      }
    }

    @Override
    public String getDocumentation() {
      return
          "Extracts a value from the JSON object based on the provided JSON path. An optional third "
              + "argument can be provided to specify a default value when the given JSON path does not yield "
              + "a value for the JSON object.";
    }
  }

  public static class JsonQuery extends ScalarFunction implements SqrlFunction {

    static ObjectMapper mapper = new ObjectMapper();

    @FunctionHint(input = {@DataTypeHint(value = "RAW", bridgedTo = JsonNode.class),
        @DataTypeHint(bridgedTo = String.class),})
    public String eval(JsonNode input, String pathSpec) {
      if (input == null) {
        return null;
      }
      try {
        ReadContext ctx = JsonPath.parse(input.toString());
        Object result = ctx.read(pathSpec);
        return mapper.writeValueAsString(result); // Convert the result back to JSON string
      } catch (Exception e) {
        e.printStackTrace();
        return null;
      }
    }

    @Override
    public String getDocumentation() {
      return "For a given JSON object, executes a JSON path query against the object and returns the result as string.";
    }
  }

  public static class JsonExists extends ScalarFunction implements SqrlFunction {

    @FunctionHint(input = {@DataTypeHint(value = "RAW", bridgedTo = JsonNode.class),
        @DataTypeHint("String")})
    public Boolean eval(JsonNode json, String path) {
      if (json == null) {
        return null;
      }
      try {
        return SqlJsonUtils.jsonExists(json.toString(), path);
      } catch (Exception e) {
        return false;
      }
    }

    @Override
    public String getDocumentation() {
      return "For a given JSON object, checks whether the provided JSON path exists";
    }
  }

  //Todo: finish jsonconcat
  public static class JsonConcat extends ScalarFunction implements SqrlFunction {

    private final ObjectMapper mapper = new ObjectMapper();

    @FunctionHint(input = {@DataTypeHint(value = "RAW", bridgedTo = JsonNode.class),
        @DataTypeHint(value = "RAW", bridgedTo = JsonNode.class)}, output = @DataTypeHint(value = "RAW", bridgedTo = JsonNode.class))
    public JsonNode eval(JsonNode json1, JsonNode json2) {
      if (json1 == null || json2 == null) {
        return null;
      }
      if (json1 instanceof ObjectNode && json2 instanceof ObjectNode) {
        return evalObj((ObjectNode) json1, (ObjectNode) json2);
      } else if (json1 instanceof ArrayNode && json2 instanceof ArrayNode) {
        return evalArr((ArrayNode) json1, (ArrayNode) json2);
      }
      throw new RuntimeException("Unknown node type in json");
    }


    public ArrayNode evalArr(ArrayNode json1, ArrayNode json2) {
      if (json1 == null || json2 == null) {
        return null;
      }

      ArrayNode jsonNode = json1.deepCopy();
      return jsonNode.addAll(json2);
    }

    public ObjectNode evalObj(ObjectNode json1, ObjectNode json2) {
      if (json1 == null || json2 == null) {
        return null;
      }
      ObjectNode jsonNode = json1.deepCopy();
      return jsonNode.setAll(json2);
    }

    @Override
    public String getDocumentation() {
      return "Merges two JSON objects into one. If two objects share the same key, the value from the later object is used.";
    }
  }

  @Value
  public static class ArrayAgg {

    @DataTypeHint(value = "RAW")
    List<JsonNode> objects;

    public void add(JsonNode value) {
      objects.add(value);
    }

    public void remove(JsonNode value) {
      objects.remove(value);
    }
  }

  @FunctionHint(output = @DataTypeHint(value = "RAW", bridgedTo = JsonNode.class))
  public static class JsonArrayAgg extends AggregateFunction<JsonNode, ArrayAgg> implements
      SqrlFunction {

    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public ArrayAgg createAccumulator() {
      return new ArrayAgg(new ArrayList<>());
    }

    @FunctionHint(input = {@DataTypeHint(inputGroup = InputGroup.ANY)})
    public void accumulate(ArrayAgg accumulator, Object value) {
      if (value instanceof JsonNode) {
        accumulator.add((JsonNode) value);
      } else {
        accumulator.add(mapper.getNodeFactory().pojoNode(value));
      }
    }

    @FunctionHint(input = {@DataTypeHint(inputGroup = InputGroup.ANY)})
    public void retract(ArrayAgg accumulator, Object value) {
      if (value instanceof JsonNode) {
        accumulator.remove((JsonNode) value);
      } else {
        accumulator.remove(mapper.getNodeFactory().pojoNode(value));
      }
    }

    @Override
    public JsonNode getValue(ArrayAgg accumulator) {
      ArrayNode arrayNode = mapper.createArrayNode();
      for (JsonNode o : accumulator.getObjects()) {
        arrayNode.add(o);
      }
      return arrayNode;
    }

    @Override
    public String getDocumentation() {
      return "Aggregation function that aggregates JSON objects into a JSON array.";
    }
  }

  @Value
  public static class ObjectAgg {

    @DataTypeHint(value = "RAW")
    Map<String, JsonNode> objects;

    public void add(String key, JsonNode value) {
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

  @FunctionHint(output = @DataTypeHint(value = "RAW", bridgedTo = JsonNode.class))
  public static class JsonObjectAgg extends AggregateFunction<JsonNode, ObjectAgg> implements
      SqrlFunction {

    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public ObjectAgg createAccumulator() {
      return new ObjectAgg(new HashMap<>());
    }

    @SneakyThrows
    @FunctionHint(input = {@DataTypeHint(bridgedTo = String.class),
        @DataTypeHint(inputGroup = InputGroup.ANY)})
    public void accumulate(ObjectAgg accumulator, String key, Object value) {
      if (value instanceof JsonNode) {
        accumulator.add(key, (JsonNode) value);
      } else {
        accumulator.add(key, mapper.getNodeFactory().pojoNode(value));
      }
    }

    @SneakyThrows
    @FunctionHint(input = {@DataTypeHint(bridgedTo = String.class),
        @DataTypeHint(inputGroup = InputGroup.ANY)})
    public void retract(ObjectAgg accumulator, String key, Object value) {
      accumulator.remove(key);
    }

    @Override
    public JsonNode getValue(ObjectAgg accumulator) {
      ObjectNode objectNode = mapper.createObjectNode();
      accumulator.getObjects().forEach(objectNode::set);
      return objectNode;
    }

    @Override
    public String getDocumentation() {
      return "Aggregation function that merges JSON objects into a single JSON object. If two JSON"
          + "objects share the same field name, the value of the later one is used in the aggregated result.";
    }
  }
}