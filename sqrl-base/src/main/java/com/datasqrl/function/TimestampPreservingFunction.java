/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.function;

public interface TimestampPreservingFunction extends SqrlFunction {

  default boolean isTimestampPreserving() {
    return true;
  }

  /**
   * Returns true if this timestamp preserving function takes a single timestamp
   * argument and preserves the order of that timestamp.
   * This allows us to map the original timestamp index to the new timestamp index
   * and pull through now-filters.
   *
   * @return
   */
  default boolean preservesSingleTimestampArgument() {
    return false;
  }
}
