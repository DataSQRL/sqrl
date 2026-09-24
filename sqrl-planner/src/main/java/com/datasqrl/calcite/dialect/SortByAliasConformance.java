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
package com.datasqrl.calcite.dialect;

import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlDelegatingConformance;

/**
 * Declares that the target database resolves a name in ORDER BY to an output column alias before an
 * input column. Calcite's {@code RelToSqlConverter} relies on this flag to replace a sort key
 * shadowed by a different output alias with its ordinal, e.g. {@code SELECT MD5(x) AS id, id AS
 * original_id FROM t ORDER BY 2} instead of {@code ORDER BY id}, which would sort by the MD5 value.
 */
public class SortByAliasConformance extends SqlDelegatingConformance {

  public SortByAliasConformance(SqlConformance delegate) {
    super(delegate);
  }

  @Override
  public boolean isSortByAlias() {
    return true;
  }
}
