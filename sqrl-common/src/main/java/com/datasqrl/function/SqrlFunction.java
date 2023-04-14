/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.function;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.util.FunctionUtil;

public interface SqrlFunction {

  String getDocumentation();

  default Name getFunctionName() {
    return FunctionUtil.getFunctionNameFromClass(this.getClass());
  }

}
