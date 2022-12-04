/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.io;

public enum ExternalDataType {
  SOURCE, SINK, SOURCE_AND_SINK;

  public boolean isSource() {
    return this == SOURCE || this == SOURCE_AND_SINK;
  }

  public boolean isSink() {
    return this == SINK || this == SOURCE_AND_SINK;
  }
}
