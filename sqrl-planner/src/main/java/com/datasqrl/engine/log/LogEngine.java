package com.datasqrl.engine.log;

import com.datasqrl.engine.ExecutionEngine;
import java.util.List;
import java.util.Optional;

import lombok.Value;
import org.apache.calcite.rel.type.RelDataTypeField;

public interface LogEngine extends ExecutionEngine {

  Log createLog(String logId, RelDataTypeField schema, List<String> primaryKey,
                Timestamp timestamp);


  @Value
  public static class Timestamp {

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
