package com.datasqrl.vector;

//import com.google.auto.service.AutoService;
import java.util.Arrays;

import org.apache.flink.connector.jdbc.converter.AbstractJdbcRowConverter.JdbcDeserializationConverter;
import org.apache.flink.connector.jdbc.converter.AbstractJdbcRowConverter.JdbcSerializationConverter;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.types.logical.LogicalType;
import org.postgresql.util.PGobject;

import com.datasqrl.type.JdbcTypeSerializer;

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
    return () -> val -> {
      var t = (FlinkVectorType)val;
      return t.getValue();
    };
  }

  @Override
  public GenericSerializationConverter<JdbcSerializationConverter> getSerializerConverter(
      LogicalType type) {
    var flinkVectorTypeSerializer = new FlinkVectorTypeSerializer();
    return () -> (val, index, statement) -> {
      if (val != null && !val.isNullAt(index)) {
        RawValueData<FlinkVectorType> object = val.getRawValue(index);
        var vec = object.toObject(flinkVectorTypeSerializer);

        if (vec != null) {
          var pgObject = new PGobject();
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
