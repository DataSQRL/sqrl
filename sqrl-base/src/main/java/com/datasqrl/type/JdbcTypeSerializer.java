package com.datasqrl.type;

import java.lang.reflect.Type;

public interface JdbcTypeSerializer<D, S> {

  String getDialect();

  Type getConversionClass();

  GenericDeserializationConverter<D> getDeserializerConverter();

  GenericSerializationConverter<S> getSerializerConverter();

  interface GenericSerializationConverter<T> {
    T create();
  }

  interface GenericDeserializationConverter<T> {
    T create();
  }
}
