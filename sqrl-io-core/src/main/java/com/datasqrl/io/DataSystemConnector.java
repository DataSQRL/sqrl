package com.datasqrl.io;

public interface DataSystemConnector {

  boolean hasSourceTimestamp();

  default boolean requiresFormat(ExternalDataType type) {
    return true;
  }

}
