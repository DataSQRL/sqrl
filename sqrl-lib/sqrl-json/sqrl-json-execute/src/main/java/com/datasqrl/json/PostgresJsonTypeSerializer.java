package com.datasqrl.json;

import com.datasqrl.type.JdbcTypeSerializer;
import com.google.auto.service.AutoService;
import java.lang.reflect.Type;
import org.apache.flink.connector.jdbc.converter.AbstractJdbcRowConverter.JdbcDeserializationConverter;
import org.apache.flink.connector.jdbc.converter.AbstractJdbcRowConverter.JdbcSerializationConverter;
import org.apache.flink.table.data.StringData;
import org.postgresql.util.PGobject;

@AutoService(JdbcTypeSerializer.class)
public class PostgresJsonTypeSerializer
    implements JdbcTypeSerializer<JdbcDeserializationConverter, JdbcSerializationConverter> {

  @Override
  public String getDialect() {
    return "postgres";
  }

  @Override
  public Type getConversionClass() {
    return FlinkJsonType.class;
  }

  @Override
  public GenericDeserializationConverter<JdbcDeserializationConverter> getDeserializerConverter() {
    return () -> (val) -> {
      FlinkJsonType t = (FlinkJsonType) val;
      return t.getJson();
    };
  }

  @Override
  public GenericSerializationConverter<JdbcSerializationConverter> getSerializerConverter() {
    return ()-> (val, index, statement) -> {
      if (val != null && !val.isNullAt(index)) {
        PGobject pgObject = new PGobject();
        pgObject.setType("json");
        StringData string = val.getRow(index, 1).getString(0);
        pgObject.setValue(string.toString());
        statement.setObject(index, pgObject);
      } else {
        statement.setObject(index, null);
      }
    };
  }
}
