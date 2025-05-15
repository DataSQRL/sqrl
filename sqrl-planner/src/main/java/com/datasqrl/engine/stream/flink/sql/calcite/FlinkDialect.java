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
package com.datasqrl.engine.stream.flink.sql.calcite;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.SqlDialect;
import org.apache.flink.sql.parser.validate.FlinkSqlConformance;

public class FlinkDialect extends SqlDialect {
  public static final SqlDialect.Context DEFAULT_CONTEXT;
  public static final SqlDialect DEFAULT;

  public FlinkDialect(SqlDialect.Context context) {
    super(context);
  }

  static {
    DEFAULT_CONTEXT =
        SqlDialect.EMPTY_CONTEXT
            .withConformance(FlinkSqlConformance.DEFAULT)
            .withDatabaseProduct(DatabaseProduct.UNKNOWN)
            .withLiteralQuoteString("'")
            .withLiteralEscapedQuoteString("`")
            .withQuotedCasing(Casing.UNCHANGED)
            .withIdentifierQuoteString("`");
    DEFAULT = new FlinkDialect(DEFAULT_CONTEXT);
  }

  @Override
  public boolean supportsImplicitTypeCoercion(RexCall call) {
    return false;
  }
}
