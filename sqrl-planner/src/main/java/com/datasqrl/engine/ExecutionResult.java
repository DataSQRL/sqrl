/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine;

import lombok.Value;

public interface ExecutionResult {

  @Value
  class Message implements ExecutionResult {

    String message;
  }
}
