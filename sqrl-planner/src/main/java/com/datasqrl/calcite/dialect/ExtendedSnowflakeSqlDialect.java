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

import com.datasqrl.calcite.Dialect;
import com.datasqrl.function.translation.SqlTranslation;
import com.datasqrl.util.ServiceLoaderDiscovery;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.dialect.SnowflakeSqlDialect;

public class ExtendedSnowflakeSqlDialect extends SnowflakeSqlDialect {
  public static final Map<String, SqlTranslation> translationMap =
      ServiceLoaderDiscovery.getAll(SqlTranslation.class).stream()
          .filter(f -> f.getDialect() == Dialect.SNOWFLAKE)
          .collect(Collectors.toMap(f -> f.getOperator().getName().toLowerCase(), f -> f));

  public static final SqlDialect.Context DEFAULT_CONTEXT;
  public static final SqlDialect DEFAULT;

  static {
    DEFAULT_CONTEXT = SqlDialect.EMPTY_CONTEXT.withDatabaseProduct(DatabaseProduct.SNOWFLAKE)
    //        .withIdentifierQuoteString("\"")
    //        .withUnquotedCasing(Casing.TO_UPPER)
    ;
    DEFAULT = new ExtendedSnowflakeSqlDialect(DEFAULT_CONTEXT);
  }

  public ExtendedSnowflakeSqlDialect(Context context) {
    super(context);
  }

  @Override
  public void unparseCall(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    if (translationMap.containsKey(call.getOperator().getName().toLowerCase())) {
      translationMap
          .get(call.getOperator().getName().toLowerCase())
          .unparse(call, writer, leftPrec, rightPrec);
      return;
    }

    super.unparseCall(writer, call, leftPrec, rightPrec);
  }
}
