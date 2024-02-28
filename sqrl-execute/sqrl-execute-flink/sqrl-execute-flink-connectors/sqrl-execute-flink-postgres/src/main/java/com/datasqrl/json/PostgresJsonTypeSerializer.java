package com.datasqrl.json;

import com.datasqrl.type.JdbcTypeSerializer;
import com.google.auto.service.AutoService;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.connector.jdbc.converter.AbstractJdbcRowConverter.JdbcDeserializationConverter;
import org.apache.flink.connector.jdbc.converter.AbstractJdbcRowConverter.JdbcSerializationConverter;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.types.logical.LogicalType;
import org.postgresql.util.PGobject;

@AutoService(JdbcTypeSerializer.class)
public class PostgresJsonTypeSerializer
    implements JdbcTypeSerializer<JdbcDeserializationConverter, JdbcSerializationConverter> {

  @Override
  public String getDialect() {
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
    return ()-> (val, index, statement) -> {
      if (val != null && !val.isNullAt(index)) {
        PGobject pgObject = new PGobject();
        pgObject.setType("json");
        RawValueData<FlinkJsonType> object = val.getRawValue(index);
        FlinkJsonType vec = object.toObject(new KryoSerializer<>
            (FlinkJsonType.class, new ExecutionConfig()));
        pgObject.setValue(vec.getJson());
        statement.setObject(index, pgObject);
      } else {
        statement.setObject(index, null);
      }
    };
  }
}
