package com.datasqrl.json;

import java.util.HashMap;
import lombok.SneakyThrows;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.table.functions.AggregateFunction;

/**
 * Aggregation function that merges JSON objects into a single JSON object. If two JSON objects
 * share the same field name, the value of the later one is used in the aggregated result.
 */
public class JsonObjectAgg extends AggregateFunction<FlinkJsonType, ObjectAgg> {

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

}