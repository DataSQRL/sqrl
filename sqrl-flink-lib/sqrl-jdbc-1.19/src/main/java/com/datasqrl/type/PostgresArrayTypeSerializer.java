package com.datasqrl.type;

import static com.datasqrl.type.FlinkArrayTypeUtil.getBaseFlinkArrayType;
import static com.datasqrl.type.FlinkArrayTypeUtil.isScalarArray;
import static com.datasqrl.type.PostgresArrayTypeConverter.getArrayScalarName;

import com.datasqrl.format.SqrlRowDataToJsonConverters;
import com.datasqrl.json.FlinkJsonType;
import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDateTime;
import lombok.SneakyThrows;
import org.apache.flink.connector.jdbc.converter.AbstractJdbcRowConverter.JdbcDeserializationConverter;
import org.apache.flink.connector.jdbc.converter.AbstractJdbcRowConverter.JdbcSerializationConverter;
import org.apache.flink.connector.jdbc.statement.FieldNamedPreparedStatement;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonFormatOptions.MapNullKeyMode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryArrayData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.postgresql.util.PGobject;

public class PostgresArrayTypeSerializer
    implements JdbcTypeSerializer<JdbcDeserializationConverter, JdbcSerializationConverter> {


  @Override
  public String getDialectId() {
    return "postgres";
  }

  @Override
  public Class getConversionClass() {
    return Object[].class;
  }

  @Override
  public String dialectTypeName() {
    return "arr";
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
    return () -> (val, index, statement) -> {
      if (val != null && !val.isNullAt(index) && !LogicalTypeRoot.NULL.equals(type.getTypeRoot())) {
        createSqlArrayObject(type, val, index, statement);
      } else {
        statement.setObject(index, null);
      }
    };
  }

  ObjectMapper mapper = new ObjectMapper();

  @SneakyThrows
  private void createSqlArrayObject(LogicalType type, RowData val, int index,
      FieldNamedPreparedStatement statement) {
    SqrlRowDataToJsonConverters rowDataToJsonConverter = new SqrlRowDataToJsonConverters(
        TimestampFormat.SQL, MapNullKeyMode.DROP, "null");
    ArrayType arrayType = (ArrayType) type;
    ArrayNode objectNode = mapper.createArrayNode();
    ArrayData object = val.getArray(index);

    JsonNode convert = rowDataToJsonConverter.createConverter(arrayType)
        .convert(mapper, objectNode, object);

    PGobject pgObject = new PGobject();
    pgObject.setType("json");
    pgObject.setValue(convert.toString());
    statement.setObject(index, pgObject);
  }
  @Override
  public boolean supportsType(LogicalType type) {
    return type instanceof ArrayType;
  }
}
