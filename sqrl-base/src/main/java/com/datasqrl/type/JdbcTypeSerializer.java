package com.datasqrl.type;

public interface JdbcTypeSerializer<D, S> {

  String getDialect();

  Class getConversionClass();

  String dialectTypeName();

  GenericDeserializationConverter<D> getDeserializerConverter();

  GenericSerializationConverter<S> getSerializerConverter();

  interface GenericSerializationConverter<T> {
    T create();
  }

  interface GenericDeserializationConverter<T> {
    T create();
  }
}
