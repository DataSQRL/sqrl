package com.datasqrl.jdbc;

import static com.datasqrl.type.FlinkArrayTypeUtil.getBaseFlinkArrayType;
import static com.datasqrl.type.FlinkArrayTypeUtil.isScalarArray;
import static com.datasqrl.type.PostgresArrayTypeConverter.getArrayScalarName;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.MAP;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.ROW;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE;

import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDateTime;
import lombok.SneakyThrows;
import org.apache.flink.connector.jdbc.converter.AbstractJdbcRowConverter;
import org.apache.flink.connector.jdbc.statement.FieldNamedPreparedStatement;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryArrayData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

/**
 * A sqrl class to handle arrays and extra data types
 */
public abstract class SqrlBaseJdbcRowConverter extends AbstractJdbcRowConverter {

  public SqrlBaseJdbcRowConverter(RowType rowType) {
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
    } else if (type.getTypeRoot() == ROW) {
      return (val, index, statement) -> setRow(type, val, index, statement);
    } else if (type.getTypeRoot() == MAP) {
      return (val, index, statement) -> setRow(type, val, index, statement);
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
    } else if (root == LogicalTypeRoot.ROW) {
      return val-> val;
    } else if (root == LogicalTypeRoot.MAP) {
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
        return (val, index, statement) -> setArray(type, val, index, statement);
      case ROW:
        return (val, index, statement) -> setRow(type, val, index, statement);
      case MAP:
        return (val, index, statement) -> setRow(type, val, index, statement);
      case MULTISET:
      case RAW:
      default:
        return super.createExternalConverter(type);
    }
  }

  public abstract void setRow(LogicalType type, RowData val, int index,
      FieldNamedPreparedStatement statement);
  @SneakyThrows
  public void setArray(LogicalType type, RowData val, int index, FieldNamedPreparedStatement statement) {
    SqrlFieldNamedPreparedStatementImpl flinkPreparedStatement = (SqrlFieldNamedPreparedStatementImpl) statement;
    for (int idx : flinkPreparedStatement.getIndexMapping()[index]) {
      ArrayData arrayData = val.getArray(index);
      createSqlArrayObject(type, arrayData, idx, flinkPreparedStatement.getStatement());
    }
  }

  @SneakyThrows
  private void createSqlArrayObject(LogicalType type, ArrayData data, int idx,
      PreparedStatement statement) {
    //Scalar arrays of any dimension are one array call
    if (isScalarArray(type)) {
      Object[] boxed;
      if (data instanceof GenericArrayData) {
        boxed = ((GenericArrayData)data).toObjectArray();
      } else if (data instanceof BinaryArrayData) {
        boxed = ((BinaryArrayData)data).toObjectArray(getBaseFlinkArrayType(type));
      } else {
        throw new RuntimeException("Unsupported ArrayData type: " + data.getClass());
      }
      Array array = statement.getConnection()
          .createArrayOf(getArrayScalarName(type), boxed);
      statement.setArray(idx, array);
    } else {
      // If it is not a scalar array (e.g. row type), use an empty byte array.
      Array array = statement.getConnection()
          .createArrayOf(getArrayType(), new Byte[0]);
      statement.setArray(idx, array);
    }
  }

  protected abstract String getArrayType();

  public abstract JdbcDeserializationConverter createArrayConverter(ArrayType arrayType);
}
