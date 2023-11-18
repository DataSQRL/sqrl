package com.datasqrl.type;

import com.datasqrl.calcite.type.FlinkVectorType;
import com.google.auto.service.AutoService;
import java.lang.reflect.Type;
import java.util.Arrays;
import org.apache.flink.connector.jdbc.converter.AbstractJdbcRowConverter.JdbcDeserializationConverter;
import org.apache.flink.connector.jdbc.converter.AbstractJdbcRowConverter.JdbcSerializationConverter;
import org.apache.flink.table.data.ArrayData;
import org.postgresql.util.PGobject;

@AutoService(JdbcTypeSerializer.class)
public class PostgresVectorTypeSerializer implements JdbcTypeSerializer {

  @Override
  public String getDialect() {
    return "postgres";
  }

  @Override
  public Type getConversionClass() {
    return FlinkVectorType.class;
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
        ArrayData string = val.getRow(index, 1).getArray(0);
        pgObject.setValue(Arrays.toString(string.toDoubleArray()));
        statement.setObject(index, pgObject);
      } else {
        statement.setObject(index, null);
      }
    };
  }
}
