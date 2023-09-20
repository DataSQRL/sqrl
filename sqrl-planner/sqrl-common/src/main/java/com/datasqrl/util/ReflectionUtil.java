package com.datasqrl.util;

import java.lang.reflect.Method;
import java.util.List;
import lombok.SneakyThrows;

public class ReflectionUtil {
  public static Method findMethod(Class<?> clazz, String name) {
    Method[] methods = clazz.getMethods();

    for(int i = 0; i < methods.length; ++i) {
      Method method = methods[i];
      if (method.getName().equals(name) && !method.isBridge()) {
        return method;
      }
    }

    return null;
  }

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
