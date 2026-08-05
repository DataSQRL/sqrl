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

import org.apache.calcite.config.NullCollation;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlAlienSystemTypeNameSpec;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.SparkSqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;

public class ExtendedSparkSqlDialect extends SparkSqlDialect {

  public static final Context DEFAULT_CONTEXT =
      SqlDialect.EMPTY_CONTEXT
          .withDatabaseProduct(DatabaseProduct.SPARK)
          .withNullCollation(NullCollation.LOW)
          .withIdentifierQuoteString("`");

  public static final SqlDialect DEFAULT = new ExtendedSparkSqlDialect(DEFAULT_CONTEXT);

  public ExtendedSparkSqlDialect(Context context) {
    super(context);
  }

  @Override
  public SqlNode getCastSpec(RelDataType type) {
    var castSpec = getSparkType(type);

    return new SqlDataTypeSpec(
        new SqlAlienSystemTypeNameSpec(castSpec, type.getSqlTypeName(), SqlParserPos.ZERO),
        SqlParserPos.ZERO);
  }

  private String getSparkType(RelDataType type) {
    String castSpec;
    switch (type.getSqlTypeName()) {
      case CHAR:
      case VARCHAR:
        castSpec = "STRING";
        break;
      case BINARY:
      case VARBINARY:
        castSpec = "BINARY";
        break;
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        castSpec = "TIMESTAMP_LTZ";
        break;
      case TIMESTAMP:
        castSpec = "TIMESTAMP_NTZ";
        break;
      case ARRAY:
        castSpec = "ARRAY<" + getSparkType(type.getComponentType()) + ">";
        break;
      case MAP:
        castSpec =
            "MAP<"
                + getSparkType(type.getKeyType())
                + ", "
                + getSparkType(type.getValueType())
                + ">";
        break;
      case ROW:
        castSpec =
            "STRUCT<"
                + type.getFieldList().stream()
                    .map(
                        field ->
                            quoteIdentifier(field.getName()) + ": " + getSparkType(field.getType()))
                    .reduce((left, right) -> left + ", " + right)
                    .orElse("")
                + ">";
        break;
      default:
        return super.getCastSpec(type).toString();
    }

    return castSpec;
  }
}
