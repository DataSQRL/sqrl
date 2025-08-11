/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasqrl.error;

public enum ErrorCode implements ErrorLabel {
  IMPORT_NAMESPACE_CONFLICT,
  IMPORT_CANNOT_BE_ALIASED,
  IMPORT_STAR_CANNOT_HAVE_TIMESTAMP,
  IMPORT_IN_HEADER,
  INVALID_IMPORT,
  INVALID_EXPORT,
  INVALID_HINT,
  INVALID_TABLE_FUNCTION_ARGUMENTS,
  INVALID_SQRL_DEFINITION,
  INVALID_SQRL_ADD_COLUMN,
  BASETABLE_ONLY_ERROR,
  MISSING_DEST_TABLE,
  TIMESTAMP_COLUMN_MISSING,
  TIMESTAMP_COLUMN_EXPRESSION,
  PATH_NOT_WRITABLE,
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
  CONFIG_EXCEPTION,
  PRIMARY_KEY_NULLABLE,
  MULTIPLE_PRIMARY_KEY,
  TABLE_LOCKED,
  SCHEMA_ERROR,
  TABLE_NOT_MATERIALIZE,
  FUNCTION_EXISTS,
  NOT_YET_IMPLEMENTED,
  NO_API_ENDPOINTS,
  MISSING_SORT_COLUMN,
  ROWTIME_IS_NULLABLE;

  @Override
  public String getLabel() {
    return toString();
  }
}
