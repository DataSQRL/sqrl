package com.datasqrl.calcite.type;

import java.util.Optional;
import org.apache.calcite.sql.SqlFunction;

public interface NonnativeType {

  Optional<SqlFunction> getDowncastFunction();
  Optional<SqlFunction> getUpcastFunction();
}
