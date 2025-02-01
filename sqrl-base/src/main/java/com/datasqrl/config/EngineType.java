package com.datasqrl.config;

public enum EngineType {
  STREAMS, DATABASE, SERVER, LOG, QUERY, EXPORT;

  public boolean isWrite() {
    return this == STREAMS;
  }

  public boolean isRead() {
    return this == DATABASE || this == SERVER;
  }

  public boolean supportsExport() {
    return isDataStore() || this == EXPORT;
  }

  public boolean isDataStore() {
    return this == DATABASE || this == LOG;
  }

  public boolean isCompute() {
    return isWrite() || isRead();
  }
}
