/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.io;

public interface DataSystemConnector {

  boolean hasSourceTimestamp();

  default boolean requiresFormat(ExternalDataType type) {
    return true;
  }

  String getPrefix();
}
