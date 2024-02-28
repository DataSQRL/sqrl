package com.datasqrl.vector;

import com.datasqrl.type.JdbcTypeSerializer;
import com.google.auto.service.AutoService;
import java.util.Arrays;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.connector.jdbc.converter.AbstractJdbcRowConverter.JdbcDeserializationConverter;
import org.apache.flink.connector.jdbc.converter.AbstractJdbcRowConverter.JdbcSerializationConverter;
import org.apache.flink.table.data.RawValueData;
import org.postgresql.util.PGobject;

@AutoService(JdbcTypeSerializer.class)
public class PostgresVectorTypeSerializer implements JdbcTypeSerializer {

  @Override
  public String getDialect() {
    return "postgres";
  }

  @Override
  public Class getConversionClass() {
    return FlinkVectorType.class;
  }

  @Override
  public String dialectTypeName() {
    return "vector";
  }

  @Override
  public GenericDeserializationConverter<JdbcDeserializationConverter> getDeserializerConverter() {
    return () -> (val)->{
      FlinkVectorType t = (FlinkVectorType)val;
      return t.getValue();
    };
  }

  @Override
  public GenericSerializationConverter<JdbcSerializationConverter> getSerializerConverter() {
    return () -> (val, index, statement) -> {
      if (val != null && !val.isNullAt(index)) {
        PGobject pgObject = new PGobject();
        pgObject.setType("vector");
        RawValueData<FlinkVectorType> object = val.getRawValue(index);
        FlinkVectorType vec = object.toObject(new KryoSerializer<>
                (FlinkVectorType.class, new ExecutionConfig()));
        pgObject.setValue(Arrays.toString(vec.getValue()));
        statement.setObject(index, pgObject);

      } else {
        statement.setObject(index, null);
      }
    };
  }
}
