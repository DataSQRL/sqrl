package com.datasqrl.json;

import java.util.LinkedHashMap;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.functions.AggregateFunction;

/**
 * Aggregation function that merges JSON objects into a single JSON object. If two JSON objects
 * share the same field name, the value of the later one is used in the aggregated result.
 */
@FunctionHint(output = @DataTypeHint(value= "RAW", bridgedTo = FlinkJsonType.class, rawSerializer = FlinkJsonTypeSerializer.class))
public class JsonObjectAgg extends AggregateFunction<Object, ObjectAgg> {

  private static final long serialVersionUID = 1L;
  private static final ObjectMapper mapper = new ObjectMapper();

  @Override
  public ObjectAgg createAccumulator() {
    return new ObjectAgg(new LinkedHashMap<>());
  }

  public void accumulate(ObjectAgg accumulator, String key, @DataTypeHint(inputGroup = InputGroup.ANY) Object value) {
    Object val = unboxFlinkJsonType(value);
    accumulator.add(key, mapper.getNodeFactory().pojoNode(val));
  }

  public void retract(ObjectAgg accumulator, String key, @DataTypeHint(inputGroup = InputGroup.ANY) Object value) {
    accumulator.remove(key);
  }

  public void merge(ObjectAgg accumulator, java.lang.Iterable<ObjectAgg> iterable) {
    iterable.forEach(o->accumulator.getObjects().putAll(o.getObjects()));
  }

  public void resetAccumulator(ObjectAgg acc) {
    acc.getObjects().clear();
  }

  @Override
  public FlinkJsonType getValue(ObjectAgg accumulator) {
    ObjectNode objectNode = mapper.createObjectNode();
    accumulator.getObjects().forEach(objectNode::putPOJO);
    return new FlinkJsonType(objectNode);
  }

  private Object unboxFlinkJsonType(Object value) {
    return (value instanceof FlinkJsonType) ? ((FlinkJsonType)value).getJson() : value;
  }
}