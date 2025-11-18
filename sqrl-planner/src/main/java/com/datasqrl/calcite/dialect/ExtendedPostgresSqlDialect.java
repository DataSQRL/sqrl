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

import static org.apache.calcite.sql.SqlKind.COLLECTION_TABLE;

import com.datasqrl.calcite.Dialect;
import com.datasqrl.flinkrunner.stdlib.json.FlinkJsonType;
import com.datasqrl.flinkrunner.stdlib.vector.FlinkVectorType;
import com.datasqrl.function.translation.SqlTranslation;
import com.datasqrl.util.ServiceLoaderDiscovery;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.sql.SqlAlienSystemTypeNameSpec;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.ArraySqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.flink.table.planner.plan.schema.RawRelDataType;

public class ExtendedPostgresSqlDialect extends PostgresqlSqlDialect {

  public static final Map<String, SqlTranslation> translationMap =
      ServiceLoaderDiscovery.getAll(SqlTranslation.class).stream()
          .filter(f -> f.getDialect() == Dialect.POSTGRES)
          .collect(Collectors.toMap(f -> f.getOperator().getName().toLowerCase(), f -> f));

  public static final ExtendedPostgresSqlDialect.Context DEFAULT_CONTEXT;
  public static final ExtendedPostgresSqlDialect DEFAULT;

  static {
    DEFAULT_CONTEXT =
        SqlDialect.EMPTY_CONTEXT
            .withDatabaseProduct(DatabaseProduct.POSTGRESQL)
            .withIdentifierQuoteString("\"")
            .withUnquotedCasing(Casing.TO_LOWER)
            .withDataTypeSystem(POSTGRESQL_TYPE_SYSTEM)
            .withConformance(new PostgresConformance());
    DEFAULT = new ExtendedPostgresSqlDialect(DEFAULT_CONTEXT);
  }

  public ExtendedPostgresSqlDialect(Context context) {
    super(context);
  }

  @Override
  public SqlConformance getConformance() {
    return new PostgresConformance();
  }

  @Override
  public SqlDataTypeSpec getCastSpec(RelDataType type) {
    String castSpec;

    if (type.getComponentType() instanceof RelRecordType) {
      castSpec = "jsonb";

    } else if (type instanceof ArraySqlType) {
      var componentSpec = getCastSpec(type.getComponentType());
      castSpec = componentSpec + " ARRAY";

    } else if (type instanceof RawRelDataType rawRelDataType) {
      Class<?> originatingClass = rawRelDataType.getRawType().getOriginatingClass();
      if (originatingClass.equals(FlinkVectorType.class)) {
        castSpec = "VECTOR";
      } else if (originatingClass.equals(FlinkJsonType.class)) {
        castSpec = "jsonb";
      } else {
        throw new RuntimeException("Could not find type name for: %s" + type);
      }
    } else {
      switch (type.getSqlTypeName()) {
        case TINYINT:
          castSpec = "SMALLINT";
          break;
        case DOUBLE:
          castSpec = "DOUBLE PRECISION";
          break;
        case CHAR:
        case VARCHAR:
          castSpec = "TEXT"; // Using TEXT type as it has similar flexibility and avoids size issues
          break;
        case DECIMAL:
          castSpec = "NUMERIC"; // DECIMAL or NUMERIC in PostgreSQL
          break;
        case TIME:
          castSpec = "TIME WITHOUT TIME ZONE";
          break;
        case TIMESTAMP:
          castSpec = "TIMESTAMP WITHOUT TIME ZONE";
          break;
        case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
          castSpec = "TIMESTAMP WITH TIME ZONE";
          break;
        case BINARY:
        case VARBINARY:
          castSpec = "BYTEA";
          break;
        case INTERVAL_YEAR_MONTH:
          castSpec = "INTERVAL YEAR TO MONTH";
          break;
        case INTERVAL_DAY:
          castSpec = "INTERVAL DAY TO SECOND";
          break;
        case NULL:
          castSpec = "NULL"; // Postgres supports the NULL type, though it's rarely used explicitly
          break;
        // May need to create user-defined types in PostgreSQL or use JSON/JSONB types
        case ROW:
        case MAP:
          castSpec = "jsonb";
          break;
        case SYMBOL:
        default:
          return (SqlDataTypeSpec) super.getCastSpec(type);
      }
    }

    return new SqlDataTypeSpec(
        new SqlAlienSystemTypeNameSpec(
            castSpec,
            type.getSqlTypeName() == null ? SqlTypeName.OTHER : type.getSqlTypeName(),
            SqlParserPos.ZERO),
        SqlParserPos.ZERO);
  }

  @Override
  public void unparseCall(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    if (call.getOperator().getKind() == COLLECTION_TABLE) { // skip FROM TABLE(..) call
      unparseCall(writer, (SqlCall) call.getOperandList().get(0), leftPrec, rightPrec);
      return;
    }

    if (translationMap.containsKey(call.getOperator().getName().toLowerCase())) {
      translationMap
          .get(call.getOperator().getName().toLowerCase())
          .unparse(call, writer, leftPrec, rightPrec);
      return;
    }

    super.unparseCall(writer, call, leftPrec, rightPrec);
  }

  @Override
  public boolean supportsGroupByLiteral() {
    return true;
  }
}
