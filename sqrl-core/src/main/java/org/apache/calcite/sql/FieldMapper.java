package org.apache.calcite.sql;

import ai.datasqrl.schema.Field;

public interface FieldMapper {
    String getName(Field field);
  }