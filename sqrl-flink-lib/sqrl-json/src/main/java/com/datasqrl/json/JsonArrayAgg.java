package com.datasqrl.json;

import java.util.ArrayList;
import lombok.SneakyThrows;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.functions.AggregateFunction;

/**
 * Aggregation function that aggregates JSON objects into a JSON array.
 */
public class JsonArrayAgg extends AggregateFunction<FlinkJsonType, ArrayAgg> {
  private static final long serialVersionUID = 1L;

  private static final ObjectMapper mapper = new ObjectMapper();

  @Override
  public ArrayAgg createAccumulator() {
    return new ArrayAgg(new ArrayList<>());
  }

  public void accumulate(ArrayAgg accumulator, String value) {
    accumulator.add(mapper.getNodeFactory().textNode(value));
  }

  @SneakyThrows
  public void accumulate(ArrayAgg accumulator, @DataTypeHint(inputGroup = InputGroup.ANY) Object value) {
    accumulator.add(mapper.getNodeFactory().pojoNode(unboxFlinkJsonType(value)));
  }


  public void retract(ArrayAgg accumulator, @DataTypeHint(inputGroup = InputGroup.ANY) Object value) {
    accumulator.remove(mapper.getNodeFactory().pojoNode(value));
  }

  public void merge(ArrayAgg accumulator, java.lang.Iterable<ArrayAgg> iterable) {
    iterable.forEach(o->accumulator.getObjects().addAll(o.getObjects()));
  }

  public void resetAccumulator(ArrayAgg acc) {
    acc.getObjects().clear();
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

  private Object unboxFlinkJsonType(Object value) {
    return value instanceof FlinkJsonType ? ((FlinkJsonType) value).getJson() : value;
  }
}