package com.datasqrl.util;

import com.datasqrl.function.FunctionMetadata;
import com.datasqrl.function.IndexableFunction;
import com.datasqrl.function.InputPreservingFunction;
import com.datasqrl.function.SqrlTimeTumbleFunction;
import com.datasqrl.function.TimestampPreservingFunction;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Optional;
import org.apache.calcite.sql.SqlOperator;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlAggFunction;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction;

public class FunctionUtil {

  public static <T> Optional<T> getFunctionByNameFromClass(Class clazz,
      Class<T> assignableFrom, String name) {
    for (Field field : clazz.getDeclaredFields()) {
      try {
        if (Modifier.isStatic(field.getModifiers()) && assignableFrom.isAssignableFrom(field.getType())) {
          if (field.getName().equalsIgnoreCase(name)) {
            return Optional.of((T)field.get(null));
          }
        }
      } catch (IllegalAccessException e) {

      }
    }
    return Optional.empty();
  }

  public static Optional<FunctionDefinition> getSqrlFunction(SqlOperator operator) {
    if (operator instanceof BridgingSqlFunction) {
      return Optional.of(((org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction)operator).getDefinition());
    } else if (operator instanceof BridgingSqlAggFunction) {
      return Optional.of(((org.apache.flink.table.planner.functions.bridging.BridgingSqlAggFunction)operator).getDefinition());
    }
    return Optional.empty();
  }

  public static Optional<TimestampPreservingFunction> getTimestampPreservingFunction(
      FunctionDefinition functionDefinition) {

    return ServiceLoaderDiscovery.getAll(FunctionMetadata.class)
        .stream()
        .filter(f->f.getMetadataClass().equals(functionDefinition.getClass()))
        .filter(f->TimestampPreservingFunction.class.isAssignableFrom(f.getClass()))
        .map(f->(TimestampPreservingFunction)f)
        .findFirst();
  }

  public static Optional<InputPreservingFunction> isInputPreservingFunction(FunctionDefinition functionDefinition) {
    return ServiceLoaderDiscovery.getAll(FunctionMetadata.class)
        .stream()
        .filter(f->f.getMetadataClass().equals(functionDefinition.getClass()))
        .filter(f->InputPreservingFunction.class.isAssignableFrom(f.getClass()))
        .map(f->(InputPreservingFunction)f)
        .findFirst();
  }
  public static Optional<SqrlTimeTumbleFunction> isTimeTumbleFunction(FunctionDefinition functionDefinition) {
    return ServiceLoaderDiscovery.getAll(FunctionMetadata.class)
        .stream()
        .filter(f->f.getMetadataClass().equals(functionDefinition.getClass()))
        .filter(f->SqrlTimeTumbleFunction.class.isAssignableFrom(f.getClass()))
        .map(f->(SqrlTimeTumbleFunction)f)
        .findFirst();
  }

  public static Optional<IndexableFunction> isIndexableFunctionMetadata(FunctionDefinition functionDefinition) {
    return ServiceLoaderDiscovery.getAll(FunctionMetadata.class).stream()
        .filter(f->f.getMetadataClass().equals(functionDefinition.getClass()))
        .filter(f->IndexableFunction.class.isAssignableFrom(f.getClass()))
        .map(f->(IndexableFunction)f)
        .findFirst();
  }
}
