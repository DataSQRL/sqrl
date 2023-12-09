package com.datasqrl.util;

import com.datasqrl.flink.function.BridgingFunction;
import com.datasqrl.function.SqrlFunction;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Optional;
import org.apache.calcite.sql.SqlOperator;
import org.apache.flink.table.functions.FunctionDefinition;

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


  public static Optional<SqrlFunction> getSqrlFunction(SqlOperator operator) {
    if (operator instanceof BridgingFunction) {
      FunctionDefinition function = ((BridgingFunction)operator).getDefinition();
      if (function instanceof SqrlFunction) {
        return Optional.of((SqrlFunction) function);
      }
    }
    return Optional.empty();
  }
}
