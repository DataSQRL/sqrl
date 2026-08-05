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
package com.datasqrl.calcite.convert;

import com.datasqrl.calcite.Dialect;
import java.util.Map;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;

/** Provides conversions to SQL representations for a specific dialect. */
public interface SqlConverters {

  /**
   * Converts a relational plan to a SQL node for this converter's dialect.
   *
   * @param relNode relational plan to convert
   * @param tableNameMapping mapping from planner table identifiers to physical table names
   * @return the dialect-specific SQL node
   */
  SqlNode convert(RelNode relNode, Map<String, String> tableNameMapping);

  /**
   * Serializes a SQL node as SQL for this converter's dialect.
   *
   * @param sqlNode SQL node to unparse
   * @return the dialect-specific SQL string
   */
  String convert(SqlNode sqlNode);

  /**
   * Returns the Calcite dialect used for conversion and dialect-specific DDL generation.
   *
   * @return the configured Calcite SQL dialect
   */
  SqlDialect getCalciteSqlDialect();

  Dialect getDialect();
}
