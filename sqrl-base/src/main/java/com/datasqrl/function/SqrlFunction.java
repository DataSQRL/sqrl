/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.function;

import com.datasqrl.canonicalizer.Name;

public interface SqrlFunction {

  String getDocumentation();

  default Name getFunctionName() {
    return getFunctionNameFromClass(this.getClass());
  }

  static Name getFunctionNameFromClass(Class clazz) {
    String fctName = clazz.getSimpleName();
    return Name.system(fctName);
  }
}
