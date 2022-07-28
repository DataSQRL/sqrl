package org.apache.calcite.sql.fun;

import java.util.Optional;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunction;

public interface ConvertableFunction {
  public Optional<SqlAggFunction> revert();
  public Optional<SqlFunction> convertToInlineAgg();
}
