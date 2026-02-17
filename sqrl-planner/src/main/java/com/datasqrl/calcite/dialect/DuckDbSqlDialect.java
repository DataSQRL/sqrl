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
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlAlienSystemTypeNameSpec;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;

public class DuckDbSqlDialect extends BasePostgresSqlDialect {

  public static final SqlDialect.Context DEFAULT_CONTEXT;
  public static final SqlDialect DEFAULT;

  private static final Map<String, SqlTranslation> TRANSLATION_MAP;

  static {
    DEFAULT_CONTEXT =
        SqlDialect.EMPTY_CONTEXT
            .withDatabaseProduct(DatabaseProduct.POSTGRESQL)
            .withIdentifierQuoteString("\"")
            .withUnquotedCasing(Casing.TO_LOWER);
    DEFAULT = new DuckDbSqlDialect(DEFAULT_CONTEXT);

    TRANSLATION_MAP =
        ServiceLoaderDiscovery.getAll(SqlTranslation.class).stream()
            .filter(f -> f.getDialect() == Dialect.DUCKDB)
            .collect(Collectors.toMap(f -> f.getOperator().getName().toLowerCase(), f -> f));
  }

  public DuckDbSqlDialect(Context context) {
    super(context);
  }

  @Override
  public SqlDataTypeSpec getCastSpec(RelDataType type) {
    String castSpec;
    switch (type.getSqlTypeName()) {
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        castSpec = "TIMESTAMP WITH TIME ZONE";
        break;
      default:
        return (SqlDataTypeSpec) super.getCastSpec(type);
    }

    return new SqlDataTypeSpec(
        new SqlAlienSystemTypeNameSpec(castSpec, type.getSqlTypeName(), SqlParserPos.ZERO),
        SqlParserPos.ZERO);
  }

  @Override
  protected Map<String, SqlTranslation> getTranslationMap() {
    return TRANSLATION_MAP;
  }
}
