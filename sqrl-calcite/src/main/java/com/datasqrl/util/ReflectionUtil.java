package com.datasqrl.util;

import java.lang.reflect.Method;
import java.util.List;

import lombok.SneakyThrows;
import lombok.experimental.UtilityClass;

@UtilityClass
public class ReflectionUtil {
  @SneakyThrows
  public static Object invokeSuperPrivateMethod(Object instance, String methodName, List<Class> clazz, Object... params)  {
    // Determine the parameter types
    Class<?>[] paramTypes = new Class<?>[clazz.size()];
    for (int i = 0; i < clazz.size(); i++) {
      paramTypes[i] = clazz.get(i);
    }

    // Retrieve the Method object
    Method method = instance.getClass().getSuperclass().getDeclaredMethod(methodName, paramTypes);

    // Make the method accessible
    method.setAccessible(true);

    // Invoke the method
    return method.invoke(instance, params);
  }
}
