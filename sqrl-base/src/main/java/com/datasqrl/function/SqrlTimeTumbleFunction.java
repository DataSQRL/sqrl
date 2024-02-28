/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.function;

public interface SqrlTimeTumbleFunction extends FunctionMetadata, TimestampPreservingFunction {

  public Specification getSpecification(long[] arguments);

  interface Specification {

    long getWindowWidthMillis();

    long getWindowOffsetMillis();

  }

}
