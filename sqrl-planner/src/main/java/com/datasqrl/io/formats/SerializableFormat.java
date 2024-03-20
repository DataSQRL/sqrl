package com.datasqrl.io.formats;

import com.datasqrl.io.tables.TableSchema;
import java.util.Map;
import java.util.Optional;

public interface SerializableFormat<IN> {

  default boolean requiresSchema() {
    return false;
  }

  IN serialize(Map<String, Object> data, Optional<TableSchema> schema) throws Exception;

  Map<String, Object> deserialize(IN data, Optional<TableSchema> schema) throws Exception;

}
