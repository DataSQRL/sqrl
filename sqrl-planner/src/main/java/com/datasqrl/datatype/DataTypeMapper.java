package com.datasqrl.datatype;

import com.datasqrl.engine.stream.flink.connector.CastFunction;
import java.util.Optional;
import org.apache.calcite.rel.type.RelDataType;

public interface DataTypeMapper {
  String getEngineName();
  boolean nativeTypeSupport(RelDataType type);
  Optional<CastFunction> convertType(RelDataType type);
}
