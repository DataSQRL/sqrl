/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.function.builtin.time;

import lombok.Value;
import org.apache.flink.table.functions.UserDefinedFunction;

@Value
public class FlinkFnc {

  String name;
  UserDefinedFunction fnc;
}
