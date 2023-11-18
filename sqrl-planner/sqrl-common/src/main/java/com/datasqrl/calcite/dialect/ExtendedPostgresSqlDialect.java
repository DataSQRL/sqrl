package com.datasqrl.calcite.dialect;


import com.datasqrl.calcite.Dialect;
import com.datasqrl.calcite.type.PrimitiveType;
import com.datasqrl.function.translations.SqlTranslation;
import com.datasqrl.util.ServiceLoaderDiscovery;
import com.google.common.base.Preconditions;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlAlienSystemTypeNameSpec;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;

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

  public ExtendedPostgresSqlDialect(Context context) {
    super(context);
  }

  public SqlDataTypeSpec getCastSpec(RelDataType type) {
    String castSpec;
    if (type instanceof PrimitiveType) {
      String physicalType = ((PrimitiveType) type).getPhysicalTypeName(Dialect.POSTGRES);
      Preconditions.checkNotNull(physicalType, "Could not find type name for: %s", type);
      castSpec = physicalType;
    } else {
      switch (type.getSqlTypeName()) {
        case TINYINT:
          castSpec = "smallint";
          break;
        case DOUBLE:
          castSpec = "double precision";
          break;
        case CHAR:
        case VARCHAR:
          castSpec = "text";// Using TEXT type as it has similar flexibility and avoids size issues
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
        case SYMBOL:
        case MAP:
          castSpec = "bytea";
          break;
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
    if (translationMap.containsKey(call.getOperator().getName().toLowerCase())) {
      translationMap.get(call.getOperator().getName().toLowerCase())
          .unparse(call, writer, leftPrec, rightPrec);
      return;
    }

    super.unparseCall(writer, call, leftPrec, rightPrec);
  }
}
