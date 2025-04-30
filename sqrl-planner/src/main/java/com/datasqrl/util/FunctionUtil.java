package com.datasqrl.util;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Optional;

import org.apache.calcite.sql.SqlOperator;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlAggFunction;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction;

import com.datasqrl.function.FunctionMetadata;
import com.datasqrl.function.SqrlTimeTumbleFunction;

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

  public static Optional<FunctionDefinition> getBridgedFunction(SqlOperator operator) {
    if (operator instanceof BridgingSqlFunction) {
      return Optional.of(((org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction)operator).getDefinition());
    } else if (operator instanceof BridgingSqlAggFunction) {
      return Optional.of(((org.apache.flink.table.planner.functions.bridging.BridgingSqlAggFunction)operator).getDefinition());
    }
    return Optional.empty();
  }

  public static<C> Optional<C> getFunctionMetaData(
          FunctionDefinition functionDefinition, Class<C> functionClass) {
    return ServiceLoaderDiscovery.getAll(FunctionMetadata.class)
            .stream()
            .filter(f->f.getMetadataClass().equals(functionDefinition.getClass()))
            .filter(f->functionClass.isAssignableFrom(f.getClass()))
            .map(f->(C)f)
            .findFirst();
  }

  public static Optional<SqrlTimeTumbleFunction> getSqrlTimeTumbleFunction(
      FunctionDefinition functionDefinition) {
    return getFunctionMetaData(functionDefinition, SqrlTimeTumbleFunction.class);
  }
}
