package com.datasqrl.function;

public interface TimestampPreservingFunction extends SqrlFunction {
  default boolean isTimestampPreserving() {
    return true;
  }
}
