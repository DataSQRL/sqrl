package com.datasqrl.type;

import com.datasqrl.format.SqrlRowDataToJsonConverters;
import org.apache.flink.connector.jdbc.converter.AbstractJdbcRowConverter.JdbcDeserializationConverter;
import org.apache.flink.connector.jdbc.converter.AbstractJdbcRowConverter.JdbcSerializationConverter;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonFormatOptions.MapNullKeyMode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.Row;
import org.postgresql.util.PGobject;

public class PostgresRowTypeSerializer
    implements JdbcTypeSerializer<JdbcDeserializationConverter, JdbcSerializationConverter> {

  @Override
  public String getDialectId() {
    return "postgres";
  }

  @Override
  public Class getConversionClass() {
    return Row[].class;
  }

  @Override
  public String dialectTypeName() {
    return "jsonb";
  }

  @Override
  public GenericDeserializationConverter<JdbcDeserializationConverter> getDeserializerConverter() {
    return () -> {
      return (val) -> null;
    };
  }

  @Override
  public GenericSerializationConverter<JdbcSerializationConverter> getSerializerConverter(
      LogicalType type) {
    ObjectMapper mapper = new ObjectMapper();
    return ()-> (val, index, statement) -> {
      if (val != null && !val.isNullAt(index)) {
        SqrlRowDataToJsonConverters rowDataToJsonConverter = new SqrlRowDataToJsonConverters(
            TimestampFormat.SQL, MapNullKeyMode.DROP, "null");

        ArrayType arrayType = (ArrayType) type;
        ObjectNode objectNode = mapper.createObjectNode();
        JsonNode convert = rowDataToJsonConverter.createConverter(arrayType.getElementType())
            .convert(mapper, objectNode, val);

        PGobject pgObject = new PGobject();
        pgObject.setType("json");
        pgObject.setValue(convert.toString());
        statement.setObject(index, pgObject);
      } else {
        statement.setObject(index, null);
      }
    };
  }
}
