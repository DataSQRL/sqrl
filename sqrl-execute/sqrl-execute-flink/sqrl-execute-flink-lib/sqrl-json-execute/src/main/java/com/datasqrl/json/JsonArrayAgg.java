package com.datasqrl.json;

import java.util.ArrayList;
import lombok.SneakyThrows;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.table.functions.AggregateFunction;

/**
 * Aggregation function that aggregates JSON objects into a JSON array.
 */
public class JsonArrayAgg extends AggregateFunction<FlinkJsonType, ArrayAgg> {

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
}