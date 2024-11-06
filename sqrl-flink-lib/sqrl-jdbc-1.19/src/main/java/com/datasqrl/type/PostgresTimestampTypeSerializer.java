package com.datasqrl.type;

import com.datasqrl.json.FlinkJsonType;
import com.datasqrl.json.FlinkJsonTypeSerializer;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDateTime;
import org.apache.flink.connector.jdbc.converter.AbstractJdbcRowConverter.JdbcDeserializationConverter;
import org.apache.flink.connector.jdbc.converter.AbstractJdbcRowConverter.JdbcSerializationConverter;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.postgresql.util.PGobject;

public class PostgresTimestampTypeSerializer
    implements JdbcTypeSerializer<JdbcDeserializationConverter, JdbcSerializationConverter> {

  @Override
  public String getDialectId() {
    return "postgres";
  }

  @Override
  public Class getConversionClass() {
    return Instant.class;
  }

  @Override
  public String dialectTypeName() {
    return "timestamp";
  }

  @Override
  public GenericDeserializationConverter<JdbcDeserializationConverter> getDeserializerConverter() {
    return () -> val ->
        val instanceof LocalDateTime
            ? TimestampData.fromLocalDateTime((LocalDateTime) val)
            : TimestampData.fromTimestamp((Timestamp) val);
  }

  @Override
  public GenericSerializationConverter<JdbcSerializationConverter> getSerializerConverter(
      LogicalType type) {
    final int tsPrecision = ((LocalZonedTimestampType) type).getPrecision();
    return () -> (val, index, statement) -> {
      if (val != null && !val.isNullAt(index) && !LogicalTypeRoot.NULL.equals(type.getTypeRoot())) {
        statement.setTimestamp(
            index, val.getTimestamp(index, tsPrecision).toTimestamp());
      } else {
        statement.setNull(index, Types.TIMESTAMP_WITH_TIMEZONE);
      }
    };
  }

  @Override
  public boolean supportsType(LogicalType type) {
    return type.getDefaultConversion() == getConversionClass();
  }
}
