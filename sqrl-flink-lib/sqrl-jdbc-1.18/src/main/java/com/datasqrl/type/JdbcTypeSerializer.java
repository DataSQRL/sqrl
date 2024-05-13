package com.datasqrl.type;


import org.apache.flink.table.types.logical.LogicalType;

public interface JdbcTypeSerializer<D, S> {

  String getDialectId();

  Class getConversionClass();

  String dialectTypeName();

  GenericDeserializationConverter<D> getDeserializerConverter();

  GenericSerializationConverter<S> getSerializerConverter(LogicalType type);

  interface GenericSerializationConverter<T> {
    T create();
  }

  interface GenericDeserializationConverter<T> {
    T create();
  }
}
