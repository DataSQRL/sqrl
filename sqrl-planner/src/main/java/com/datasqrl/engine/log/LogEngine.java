package com.datasqrl.engine.log;

import com.datasqrl.config.PackageJson.EngineConfig;
import com.datasqrl.engine.ExecutionEngine;

import lombok.Value;

public interface LogEngine extends ExecutionEngine {
  EngineConfig getEngineConfig();

  @Value
  class Timestamp {

    public static final Timestamp NONE = new Timestamp(null, TimestampType.NONE);

    String name;
    TimestampType type;

  }

  enum TimestampType {
    /**
     * Don't set a timestamp on the created log.
     * Timestamp name is ignored.
     */
    NONE,
    /**
     * Use the insertion time from the log.
     * This timestamp will be given the provided name. It must be unique.
     */
    LOG_TIME,
    /**
     * Use a timestamp from the data as the insertion timestamp.
     */
    SET_TIME;
  }

}
