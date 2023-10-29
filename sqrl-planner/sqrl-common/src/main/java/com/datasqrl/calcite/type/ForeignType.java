package com.datasqrl.calcite.type;

import com.datasqrl.calcite.Dialect;
import java.lang.reflect.Type;
import java.util.Optional;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlFunction;

public interface ForeignType extends RelDataType, SqrlType {

  Optional<SqlFunction> getDowncastFunction();
  Optional<SqlFunction> getUpcastFunction();

  boolean translates(Dialect dialect, RelDataType engineType);

  RelDataType getEngineType(Dialect dialect);

  Type getConversionClass();
}
