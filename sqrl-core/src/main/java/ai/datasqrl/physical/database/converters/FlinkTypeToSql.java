package ai.datasqrl.physical.database.converters;

import com.google.common.base.Preconditions;
import org.apache.flink.table.api.Schema.UnresolvedPhysicalColumn;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.CollectionDataType;

public class FlinkTypeToSql {

  public static String toSql(UnresolvedPhysicalColumn column) {
    Preconditions.checkState(!(column.getDataType() instanceof CollectionDataType),
        "Collection column encountered");

    AtomicDataType type = (AtomicDataType) column.getDataType();

    return toSql(column.getName(), getSQLType(type), !type.getLogicalType().isNullable());
  }

  private static String toSql(String name, String sqlType, boolean isNonNull) {
    StringBuilder sql = new StringBuilder();
    sql.append(name).append(" ").append(sqlType).append(" ");
    if (isNonNull) {
      sql.append("NOT NULL");
    }
    return sql.toString();
  }

  private static String getSQLType(AtomicDataType type) {
    switch (type.getLogicalType().getTypeRoot()) {
      case BOOLEAN:
      case BINARY:
      case VARBINARY:
      case DECIMAL:
      case TINYINT:
      case SMALLINT:
      case BIGINT:
      case INTEGER:
        return "BIGINT";
      case CHAR:
      case VARCHAR:
        return "VARCHAR";
      case FLOAT:
      case DOUBLE:
        return "FLOAT";
      case DATE:
        return "DATE";
      case TIME_WITHOUT_TIME_ZONE:
        return "TIME";
      case TIMESTAMP_WITHOUT_TIME_ZONE:
        return "TIMESTAMP";
      case TIMESTAMP_WITH_TIME_ZONE:
        return "TIMESTAMPTZ";
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        return "TIMESTAMPTZ";
      case INTERVAL_YEAR_MONTH:
      case INTERVAL_DAY_TIME:
      case DISTINCT_TYPE:
      case STRUCTURED_TYPE:
      case NULL:
      case SYMBOL:
      case UNRESOLVED:
      case ARRAY:
      case MAP:
      case MULTISET:
      case ROW:
      case RAW:
      default:
        throw new UnsupportedOperationException("Unsupported type:" + type);
    }
  }
}
