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
package com.datasqrl.config;

import java.util.Arrays;
import java.util.Optional;

public enum JdbcDialect {
  Postgres("PostgreSQL"),
  Oracle,
  MySQL,
  SQLServer,
  H2,
  SQLite,
  Iceberg,
  Snowflake,
  DuckDB;

  private final String[] synonyms;

  private JdbcDialect(String... synonyms) {
    this.synonyms = synonyms;
  }

  private boolean matches(String dialect) {
    if (name().equalsIgnoreCase(dialect)) {
      return true;
    }
    for (String synonym : synonyms) {
      if (synonym.equalsIgnoreCase(dialect)) {
        return true;
      }
    }
    return false;
  }

  public String getId() {
    return name().toLowerCase();
  }

  public static Optional<JdbcDialect> find(String dialect) {
    return Arrays.stream(values()).filter(d -> d.matches(dialect)).findFirst();
  }
}
