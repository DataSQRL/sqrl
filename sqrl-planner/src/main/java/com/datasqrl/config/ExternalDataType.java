/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.config;

public enum ExternalDataType {
  source, sink, source_and_sink;

  public boolean isSource() {
    return this == source || this == source_and_sink;
  }

  public boolean isSink() {
    return this == sink || this == source_and_sink;
  }
}
