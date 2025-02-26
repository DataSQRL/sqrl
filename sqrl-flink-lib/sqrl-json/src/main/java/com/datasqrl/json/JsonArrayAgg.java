package com.datasqrl.json;

import java.util.ArrayList;
import lombok.SneakyThrows;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.util.jackson.JacksonMapperFactory;

/** Aggregation function that aggregates JSON objects into a JSON array. */
public class JsonArrayAgg extends AggregateFunction<FlinkJsonType, ArrayAgg> {

  private static final ObjectMapper mapper = JacksonMapperFactory.createObjectMapper();

  @Override
  public ArrayAgg createAccumulator() {
    return new ArrayAgg(new ArrayList<>());
  }

  public void accumulate(ArrayAgg accumulator, String value) {
    accumulator.add(mapper.getNodeFactory().textNode(value));
  }

  @SneakyThrows
  public void accumulate(ArrayAgg accumulator, FlinkJsonType value) {
    if (value != null) {
      accumulator.add(value.json);
    } else {
      accumulator.add(null);
    }
  }

  public void accumulate(ArrayAgg accumulator, Double value) {
    accumulator.add(mapper.getNodeFactory().numberNode(value));
  }

  public void accumulate(ArrayAgg accumulator, Long value) {
    accumulator.add(mapper.getNodeFactory().numberNode(value));
  }

  public void accumulate(ArrayAgg accumulator, Integer value) {
    accumulator.add(mapper.getNodeFactory().numberNode(value));
  }

  public void retract(ArrayAgg accumulator, String value) {
    accumulator.remove(mapper.getNodeFactory().textNode(value));
  }

  @SneakyThrows
  public void retract(ArrayAgg accumulator, FlinkJsonType value) {
    if (value != null) {
      accumulator.remove(value.json);
    } else {
      accumulator.remove(null);
    }
  }

  public void retract(ArrayAgg accumulator, Double value) {
    accumulator.remove(mapper.getNodeFactory().numberNode(value));
  }

  public void retract(ArrayAgg accumulator, Long value) {
    accumulator.remove(mapper.getNodeFactory().numberNode(value));
  }

  public void retract(ArrayAgg accumulator, Integer value) {
    accumulator.remove(mapper.getNodeFactory().numberNode(value));
  }

  public void merge(ArrayAgg accumulator, java.lang.Iterable<ArrayAgg> iterable) {
    iterable.forEach(o -> accumulator.getObjects().addAll(o.getObjects()));
  }

  @Override
  public FlinkJsonType getValue(ArrayAgg accumulator) {
    ArrayNode arrayNode = mapper.createArrayNode();
    for (Object o : accumulator.getObjects()) {
      if (o instanceof FlinkJsonType) {
        arrayNode.add(((FlinkJsonType) o).json);
      } else {
        arrayNode.addPOJO(o);
      }
    }
    return new FlinkJsonType(arrayNode);
  }
}
