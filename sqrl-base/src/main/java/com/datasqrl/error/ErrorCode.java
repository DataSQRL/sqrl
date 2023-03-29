/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.error;

public enum ErrorCode implements ErrorLabel {

  IMPORT_NAMESPACE_CONFLICT,
  IMPORT_CANNOT_BE_ALIASED,
  IMPORT_STAR_CANNOT_HAVE_TIMESTAMP,
  IMPORT_IN_HEADER,
  MISSING_DEST_TABLE,
  TIMESTAMP_COLUMN_MISSING,
  TIMESTAMP_COLUMN_EXPRESSION,
  PATH_CONTAINS_RELATIONSHIP,
  MISSING_FIELD,
  MISSING_TABLE,
  ORDINAL_NOT_SUPPORTED,
  CANNOT_SHADOW_RELATIONSHIP,
  TO_MANY_PATH_NOT_ALLOWED,
  NESTED_DISTINCT_ON,
  CANNOT_RESOLVE_TABLESINK,
  IOEXCEPTION,
  DISTINCT_ON_TIMESTAMP,
  WRONG_TABLE_TYPE,
  WRONG_INTERVAL_JOIN,
  NOT_YET_IMPLEMENTED;

  @Override
  public String getLabel() {
    return toString();
  }
}
