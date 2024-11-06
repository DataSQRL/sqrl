package com.datasqrl.type;


import org.apache.flink.table.types.logical.LogicalType;

public interface JdbcTypeSerializer<D, S> {

  String getDialectId();

  Class getConversionClass();

  String dialectTypeName();

  GenericDeserializationConverter<D> getDeserializerConverter();

  GenericSerializationConverter<S> getSerializerConverter(LogicalType type);

  default boolean supportsType(LogicalType type) {
    return type.getDefaultConversion() == getConversionClass();
  }

  /**
   * SQRL serializers must do null handling
   */
  interface GenericSerializationConverter<T> {
    T create();
  }

  interface GenericDeserializationConverter<T> {
    T create();
  }
}
