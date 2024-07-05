package com.datasqrl.engine.log;

import com.datasqrl.canonicalizer.Name;
import java.util.List;
import lombok.Value;
import org.apache.calcite.rel.type.RelDataType;

/**
 * Interface for creating logs during the parsing of SQRL scripts and APIs.
 */
public interface LogFactory {

  /**
   * Creates a new log.
   *
   * @param logId The unique identifier for this log. Must be globally unique.
   * @param logName The user provided name to identify this log.
   * @param schema The schema for the messages in the log.
   * @param primaryKey The primary key for messages in the log. The primary key columns must be part of the schema.
   * @param timestamp The timestamp for the messages in the log.
   * @return
   */
  Log create(String logId, Name logName, RelDataType schema, List<String> primaryKey,
      Timestamp timestamp);

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

  @Value
  class Timestamp {

    public static final Timestamp NONE = new Timestamp(null, TimestampType.NONE);

    String name;
    TimestampType type;

  }
}
