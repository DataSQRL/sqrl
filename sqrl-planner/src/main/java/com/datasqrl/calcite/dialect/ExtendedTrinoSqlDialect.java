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

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.NullCollation;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.dialect.PrestoSqlDialect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.RelToSqlConverterUtil;

/**
 * Temporary copy of Calcite 1.41's Trino dialect for the Calcite version bundled with Flink.
 * Replace this class with {@code TrinoSqlDialect} when Flink upgrades Calcite.
 */
public class ExtendedTrinoSqlDialect extends PrestoSqlDialect {

  public static final Context DEFAULT_CONTEXT =
      // TRINO was added to Calcite's DatabaseProduct enum in 1.41. Use Presto's equivalent
      // context until Flink upgrades Calcite.
      PrestoSqlDialect.DEFAULT_CONTEXT
          .withIdentifierQuoteString("\"")
          .withUnquotedCasing(Casing.UNCHANGED)
          .withNullCollation(NullCollation.LAST);

  public static final ExtendedTrinoSqlDialect DEFAULT =
      new ExtendedTrinoSqlDialect(DEFAULT_CONTEXT);

  public ExtendedTrinoSqlDialect(Context context) {
    super(context);
  }

  @Override
  public void unparseOffsetFetch(SqlWriter writer, SqlNode offset, SqlNode fetch) {
    unparseFetchUsingAnsi(writer, offset, fetch);
  }

  @Override
  public void unparseCall(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    if (call.getOperator() == SqlStdOperatorTable.SUBSTRING) {
      // Bypass Presto's SUBSTR rendering; Trino uses the standard SUBSTRING spelling.
      RelToSqlConverterUtil.specialOperatorByName("SUBSTRING").unparse(writer, call, 0, 0);
    } else if (call.getOperator().getName().equalsIgnoreCase("APPROX_COUNT_DISTINCT")) {
      RelToSqlConverterUtil.specialOperatorByName("APPROX_DISTINCT").unparse(writer, call, 0, 0);
    } else {
      super.unparseCall(writer, call, leftPrec, rightPrec);
    }
  }
}
