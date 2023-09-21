package com.datasqrl.engine.database.relational.ddl;

import com.datasqrl.calcite.Dialect;
import com.datasqrl.calcite.type.PrimitiveType;
import com.google.common.base.Preconditions;
import org.apache.calcite.rel.type.RelDataType;

public class SqrlToPostgresTypeMapper {

  public static String getSQLType(RelDataType type) {
    if (type instanceof PrimitiveType) {
      String physicalType = ((PrimitiveType) type).getPhysicalType(Dialect.POSTGRES);
      Preconditions.checkNotNull(physicalType, "Could not find type name for: %s", type);
      return physicalType;
    }
    switch (type.getSqlTypeName()) {
      case BOOLEAN:
        return "BOOLEAN";
      case TINYINT:
        return "SMALLINT"; // PostgreSQL doesn't have TINYINT, using SMALLINT instead
      case SMALLINT:
        return "SMALLINT";
      case INTEGER:
        return "INTEGER";
      case BIGINT:
        return "BIGINT";
      case CHAR:
      case VARCHAR:
        return "TEXT"; // Using TEXT type as it has similar flexibility and avoids size issues
      case DECIMAL:
        return "NUMERIC"; // DECIMAL or NUMERIC in PostgreSQL
      case DOUBLE:
        return "DOUBLE PRECISION";
      case DATE:
        return "DATE";
      case TIME:
        return "TIME WITHOUT TIME ZONE";
      case TIMESTAMP:
        return "TIMESTAMP WITHOUT TIME ZONE";
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        return "TIMESTAMP WITH TIME ZONE";
      case ARRAY:
        return getSQLType(type.getComponentType()) + "[]";
      case BINARY:
      case VARBINARY:
        return "BYTEA";
      case INTERVAL_YEAR_MONTH:
        return "INTERVAL YEAR TO MONTH";
      case INTERVAL_DAY:
        return "INTERVAL DAY TO SECOND";
      case NULL:
        return "NULL"; // Postgres supports the NULL type, though it's rarely used explicitly
      // May need to create user-defined types in PostgreSQL or use JSON/JSONB types
      case SYMBOL:
      case MAP:
      case MULTISET:
      case ROW:
      default:
        throw new UnsupportedOperationException("Unsupported type:" + type);
    }
  }
}
