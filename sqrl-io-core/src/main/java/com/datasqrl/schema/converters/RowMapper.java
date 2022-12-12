package com.datasqrl.schema.converters;

import com.datasqrl.io.SourceRecord;

/**
 * Generic row mapper for the engine
 */
public interface RowMapper<R> {
  public R apply(SourceRecord.Named sourceRecord);
}
