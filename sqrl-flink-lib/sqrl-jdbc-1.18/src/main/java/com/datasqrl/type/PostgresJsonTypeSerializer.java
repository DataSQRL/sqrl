package com.datasqrl.type;

import org.apache.flink.connector.jdbc.converter.AbstractJdbcRowConverter.JdbcDeserializationConverter;
import org.apache.flink.connector.jdbc.converter.AbstractJdbcRowConverter.JdbcSerializationConverter;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.types.logical.LogicalType;
import org.postgresql.util.PGobject;

import com.datasqrl.types.json.FlinkJsonType;
import com.datasqrl.types.json.FlinkJsonTypeSerializer;

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
    return () -> (val) -> {
      FlinkJsonType t = (FlinkJsonType) val;
      return t.getJson();
    };
  }

  @Override
  public GenericSerializationConverter<JdbcSerializationConverter> getSerializerConverter(
      LogicalType type) {
    FlinkJsonTypeSerializer typeSerializer = new FlinkJsonTypeSerializer();

    return ()-> (val, index, statement) -> {
      if (val != null && !val.isNullAt(index)) {
        PGobject pgObject = new PGobject();
        pgObject.setType("json");
        RawValueData<FlinkJsonType> object = val.getRawValue(index);
        FlinkJsonType vec = object.toObject(typeSerializer);
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
