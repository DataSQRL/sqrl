/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.function;

public interface TimestampPreservingFunction extends FunctionMetadata {

  default boolean isTimestampPreserving() {
    return true;
  }
}
