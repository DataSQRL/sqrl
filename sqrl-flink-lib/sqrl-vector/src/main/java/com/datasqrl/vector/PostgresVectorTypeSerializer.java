package com.datasqrl.vector;

import com.datasqrl.type.JdbcTypeSerializer;
import java.util.Arrays;
import org.apache.flink.connector.jdbc.converter.AbstractJdbcRowConverter.JdbcDeserializationConverter;
import org.apache.flink.connector.jdbc.converter.AbstractJdbcRowConverter.JdbcSerializationConverter;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.types.logical.LogicalType;
import org.postgresql.util.PGobject;

public class PostgresVectorTypeSerializer implements JdbcTypeSerializer {

  @Override
  public String getDialectId() {
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
  public GenericSerializationConverter<JdbcSerializationConverter> getSerializerConverter(
      LogicalType type) {
    FlinkVectorTypeSerializer flinkVectorTypeSerializer = new FlinkVectorTypeSerializer();
    return () -> (val, index, statement) -> {
      if (val != null && !val.isNullAt(index)) {
        RawValueData<FlinkVectorType> object = val.getRawValue(index);
        FlinkVectorType vec = object.toObject(flinkVectorTypeSerializer);

        if (vec != null) {
          PGobject pgObject = new PGobject();
          pgObject.setType("vector");
          pgObject.setValue(Arrays.toString(vec.getValue()));
          statement.setObject(index, pgObject);
          return;
        }
      }
      statement.setObject(index, null);
    };
  }
}
