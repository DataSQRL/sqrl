package com.datasqrl.function.builtin;

import com.datasqrl.name.Name;

public class FunctionUtil {

  public static Name getFunctionNameFromClass(Class clazz) {
    String fctName = clazz.getSimpleName();
    fctName = Character.toLowerCase(fctName.charAt(0)) + fctName.substring(1);
    return Name.system(fctName);
  }

}
