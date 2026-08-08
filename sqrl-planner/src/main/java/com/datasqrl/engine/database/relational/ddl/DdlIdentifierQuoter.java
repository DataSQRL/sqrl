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
package com.datasqrl.engine.database.relational.ddl;

import jakarta.annotation.Nullable;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.apache.calcite.sql.SqlDialect;

/**
 * Quotes DDL identifiers using the configured Calcite SQL dialect, or double quotes when no dialect
 * is configured.
 */
@RequiredArgsConstructor
public final class DdlIdentifierQuoter {

  @Nullable private final SqlDialect dialect;

  public DdlIdentifierQuoter() {
    this(null);
  }

  public String quote(String identifier) {
    if (dialect != null) {
      return dialect.quoteIdentifier(identifier);
    }

    return "\"" + identifier + "\"";
  }

  public List<String> quoteAll(List<String> identifiers) {
    return identifiers.stream().map(this::quote).toList();
  }
}
