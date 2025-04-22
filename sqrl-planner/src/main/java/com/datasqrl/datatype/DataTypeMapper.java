package com.datasqrl.datatype;

import java.util.Optional;

import org.apache.calcite.rel.type.RelDataType;

import com.datasqrl.engine.stream.flink.connector.CastFunction;

/**
 * @Deprecated Use {@link DataTypeMapping} instead
 */
public interface DataTypeMapper {
  String getEngineName();
  boolean nativeTypeSupport(RelDataType type);
  Optional<CastFunction> convertType(RelDataType type);
}
