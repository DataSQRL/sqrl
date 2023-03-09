/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.function;

import com.datasqrl.function.builtin.FunctionUtil;
import com.datasqrl.name.Name;

public interface SqrlFunction {

  String getDocumentation();

  default Name getFunctionName() {
    return FunctionUtil.getFunctionNameFromClass(this.getClass());
  }

}
