package com.datasqrl.calcite.dialect;


import static org.apache.calcite.sql.SqlKind.COLLECTION_TABLE;

import com.datasqrl.calcite.Dialect;
import com.datasqrl.function.translations.SqlTranslation;
import com.datasqrl.type.JdbcTypeSerializer;
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
import org.apache.calcite.sql.fun.SqlCollectionTableOperator;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.planner.plan.schema.RawRelDataType;

public class ExtendedPostgresSqlDialect extends PostgresqlSqlDialect {

  public static final Map<String, SqlTranslation> translationMap = ServiceLoaderDiscovery.getAll(SqlTranslation.class)
      .stream().filter(f->f.getDialect() == Dialect.POSTGRES)
      .collect(Collectors.toMap(f->f.getOperator().getName().toLowerCase(), f->f));

  public static final ExtendedPostgresSqlDialect.Context DEFAULT_CONTEXT;
  public static final ExtendedPostgresSqlDialect DEFAULT;

  static {
    DEFAULT_CONTEXT = SqlDialect.EMPTY_CONTEXT.withDatabaseProduct(DatabaseProduct.POSTGRESQL)
        .withIdentifierQuoteString("\"").withUnquotedCasing(Casing.TO_LOWER)
        .withDataTypeSystem(POSTGRESQL_TYPE_SYSTEM);
    DEFAULT = new ExtendedPostgresSqlDialect(DEFAULT_CONTEXT);
  }

  private static final Map<Class, String> foreignCastSpecMap = getForeignCastSpecs();

  public ExtendedPostgresSqlDialect(Context context) {
    super(context);
  }

  private static Map<Class, String> getForeignCastSpecs() {
    Map<Class, String> jdbcTypeSerializer = ServiceLoaderDiscovery.getAll(JdbcTypeSerializer.class)
        .stream()
        .filter(f->f.getDialectId().equalsIgnoreCase(Dialect.POSTGRES.name()))
        .collect(Collectors.toMap(JdbcTypeSerializer::getConversionClass,
            JdbcTypeSerializer::dialectTypeName));
    return jdbcTypeSerializer;
  }

  public SqlDataTypeSpec getCastSpec(RelDataType type) {
    String castSpec;
    if (type.getComponentType() instanceof RelRecordType) {
      castSpec = "jsonb";
    } else if (type instanceof RawRelDataType) {
      RawRelDataType rawRelDataType = (RawRelDataType) type;
      Class<?> originatingClass = rawRelDataType.getRawType().getOriginatingClass();
      if (foreignCastSpecMap.containsKey(originatingClass)) {
        castSpec = foreignCastSpecMap.get(originatingClass);
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
          castSpec = "TEXT";// Using TEXT type as it has similar flexibility and avoids size issues
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
        case ARRAY:
          castSpec = getCastSpec(type.getComponentType()) + "[]";
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
          castSpec = "jsonb";
          break;
        case SYMBOL:
        case MAP:
        default:
          return (SqlDataTypeSpec) super.getCastSpec(type);
      }
    }

    return new SqlDataTypeSpec(
        new SqlAlienSystemTypeNameSpec(castSpec, type.getSqlTypeName() == null ? SqlTypeName.OTHER : type.getSqlTypeName(),
            SqlParserPos.ZERO),
        SqlParserPos.ZERO);
  }

  @Override
  public void unparseCall(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    if (call.getOperator().getKind() == COLLECTION_TABLE) { //skip FROM TABLE(..) call
      unparseCall(writer, (SqlCall)call.getOperandList().get(0), leftPrec, rightPrec);
      return;
    }

    if (call.getOperator().getName().equalsIgnoreCase("allow_moved_paths")) {
      System.out.println();
    }

    if (translationMap.containsKey(call.getOperator().getName().toLowerCase())) {
      translationMap.get(call.getOperator().getName().toLowerCase())
          .unparse(call, writer, leftPrec, rightPrec);
      return;
    }

    super.unparseCall(writer, call, leftPrec, rightPrec);
  }
}
