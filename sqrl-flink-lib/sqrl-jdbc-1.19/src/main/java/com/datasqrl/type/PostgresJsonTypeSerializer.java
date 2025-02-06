package com.datasqrl.type;

import org.apache.flink.connector.jdbc.converter.AbstractJdbcRowConverter.JdbcDeserializationConverter;
import org.apache.flink.connector.jdbc.converter.AbstractJdbcRowConverter.JdbcSerializationConverter;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.types.logical.LogicalType;
import org.postgresql.util.PGobject;

import com.datasqrl.json.FlinkJsonType;
import com.datasqrl.json.FlinkJsonTypeSerializer;

public class PostgresJsonTypeSerializer
    implements JdbcTypeSerializer<JdbcDeserializationConverter, JdbcSerializationConverter> {

  @Override
  public String getDialectId() {
    return "postgres";
  }

  @Override
  public Class getConversionClass() {
    return FlinkJsonType.class;
  }

  @Override
  public String dialectTypeName() {
    return "jsonb";
  }

  @Override
  public GenericDeserializationConverter<JdbcDeserializationConverter> getDeserializerConverter() {
    return () -> val -> {
      var t = (FlinkJsonType) val;
      return t.getJson();
    };
  }

  @Override
  public GenericSerializationConverter<JdbcSerializationConverter> getSerializerConverter(
      LogicalType type) {
    var typeSerializer = new FlinkJsonTypeSerializer();

    return ()-> (val, index, statement) -> {
      if (val != null && !val.isNullAt(index)) {
        var pgObject = new PGobject();
        pgObject.setType("json");
        RawValueData<FlinkJsonType> object = val.getRawValue(index);
        var vec = object.toObject(typeSerializer);
        if (vec == null) {
          statement.setObject(index, null);
        } else {
          pgObject.setValue(vec.getJson().toString());
          statement.setObject(index, pgObject);
        }
      } else {
        statement.setObject(index, null);
      }
    };
  }
}
