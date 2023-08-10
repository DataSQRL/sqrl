package com.datasqrl.calcite.type;


public interface DelegatingDataType {

  Class<?> getConversionClass();
}
