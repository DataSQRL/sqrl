package com.datasqrl.util;

import com.datasqrl.canonicalizer.Name;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Optional;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;

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

  public static Name getFunctionNameFromClass(Class clazz) {
    String fctName = clazz.getSimpleName();
    fctName = Character.toLowerCase(fctName.charAt(0)) + fctName.substring(1);
    return Name.system(fctName);
  }

}
