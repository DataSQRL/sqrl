package com.datasqrl.json;

public interface NativeType<T> {
  Class getType();

  // Downcast to a SQL type if a connector cannot natively handle it
  T unknownTypeDowncast();

  String getPhysicalTypeName(String name);
}
