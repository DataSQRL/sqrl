package com.datasqrl.flink.function;

import com.datasqrl.calcite.type.TypeFactory;
import org.apache.calcite.sql.SqlOperatorBinding;

public class FlinkTypeUtil {

  public static TypeFactory unwrapTypeFactory(SqlOperatorBinding binding) {
    return (TypeFactory) binding.getTypeFactory();
  }
}
