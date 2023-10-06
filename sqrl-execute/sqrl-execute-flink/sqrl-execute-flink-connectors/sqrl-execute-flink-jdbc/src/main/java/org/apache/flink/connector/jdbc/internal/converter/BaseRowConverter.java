package org.apache.flink.connector.jdbc.internal.converter;

import static org.apache.flink.table.types.logical.LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE;

import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDateTime;
import org.apache.flink.connector.jdbc.converter.AbstractJdbcRowConverter;
import org.apache.flink.connector.jdbc.statement.FieldNamedPreparedStatement;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

/**
 * A sqrl class to handle arrays and extra data types
 */
public abstract class BaseRowConverter extends AbstractJdbcRowConverter {

  public BaseRowConverter(RowType rowType) {
    super(rowType);
  }


  @Override
  protected JdbcSerializationConverter wrapIntoNullableExternalConverter(
      JdbcSerializationConverter jdbcSerializationConverter, LogicalType type) {
    if (type.getTypeRoot() == TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
      int timestampWithTimezone = Types.TIMESTAMP_WITH_TIMEZONE;
      return (val, index, statement) -> {
        if (val == null
            || val.isNullAt(index)
            || LogicalTypeRoot.NULL.equals(type.getTypeRoot())) {
          statement.setNull(index, timestampWithTimezone);
        } else {
          jdbcSerializationConverter.serialize(val, index, statement);
        }
      };
    }
    return super.wrapIntoNullableExternalConverter(jdbcSerializationConverter, type);
  }
  @Override
  public JdbcDeserializationConverter createInternalConverter(LogicalType type) {
    LogicalTypeRoot root = type.getTypeRoot();

    if (root == LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
      return val ->
          val instanceof LocalDateTime
              ? TimestampData.fromLocalDateTime((LocalDateTime) val)
              : TimestampData.fromTimestamp((Timestamp) val);
    } else if (root == LogicalTypeRoot.ARRAY) {
      ArrayType arrayType = (ArrayType) type;
      return createArrayConverter(arrayType);
    } if (root == LogicalTypeRoot.ROW) {
      return val-> val;
    } else {
      return super.createInternalConverter(type);
    }
  }


  @Override
  protected JdbcSerializationConverter createExternalConverter(LogicalType type) {
    switch (type.getTypeRoot()) {
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        final int tsPrecision = ((LocalZonedTimestampType) type).getPrecision();
        return (val, index, statement) ->
            statement.setTimestamp(
                index, val.getTimestamp(index, tsPrecision).toTimestamp());
      case ARRAY:
        return (val, index, statement) -> createArraySerializer(type, val, index, statement);
      case ROW:
        return (val, index, statement) -> createRowSerializer(type, val, index, statement);
      case MAP:
      case MULTISET:
      case RAW:
      default:
        return super.createExternalConverter(type);
    }
  }

  public abstract void createRowSerializer(LogicalType type, RowData val, int index,
      FieldNamedPreparedStatement statement);
  public abstract void createArraySerializer(LogicalType type, RowData val, int index, FieldNamedPreparedStatement statement);

  public abstract JdbcDeserializationConverter createArrayConverter(ArrayType arrayType);

  public static boolean isScalarArray(LogicalType type) {
    if (type instanceof ArrayType) {
      LogicalType elementType = ((ArrayType) type).getElementType();
      return isScalar(elementType) || isScalarArray(elementType);
    }
    return false;
  }

  public static boolean isScalar(LogicalType type) {
    switch (type.getTypeRoot()) {
      case BOOLEAN:
      case TINYINT:
      case SMALLINT:
      case INTEGER:
      case BIGINT:
      case FLOAT:
      case DOUBLE:
      case CHAR:
      case VARCHAR:
      case BINARY:
      case VARBINARY:
      case DATE:
      case TIME_WITHOUT_TIME_ZONE:
      case TIMESTAMP_WITH_TIME_ZONE:
      case TIMESTAMP_WITHOUT_TIME_ZONE:
      case DECIMAL:
        return true;
      default:
        return false;
    }
  }

}
