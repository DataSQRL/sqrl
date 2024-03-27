/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.function;

public interface DocumentedFunction {

  String getDocumentation();

  default String getFunctionName() {
    return getFunctionNameFromClass(this.getClass());
  }

  static String getFunctionNameFromClass(Class clazz) {
    String fctName = clazz.getSimpleName();
    fctName = Character.toLowerCase(fctName.charAt(0)) + fctName.substring(1);
    return fctName;
  }
}
