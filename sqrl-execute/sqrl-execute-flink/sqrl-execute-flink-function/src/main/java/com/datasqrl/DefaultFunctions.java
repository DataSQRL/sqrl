package com.datasqrl;

import com.datasqrl.calcite.type.TypeFactory;
import com.datasqrl.flink.FlinkConverter;
import java.util.Map;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlFunction;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;

public class DefaultFunctions {
  public static final FlinkConverter converter = new FlinkConverter(new RexBuilder(new TypeFactory()), new TypeFactory());
  public static SqlFunction COALESCE;
  public static SqlFunction GREATEST;
  public static SqlFunction LEAST;
  public static SqlFunction NOW;
  static {
    COALESCE = converter
        .convertFunction(BuiltInFunctionDefinitions.COALESCE.getName(), BuiltInFunctionDefinitions.COALESCE.getName(), BuiltInFunctionDefinitions.COALESCE);
    GREATEST = converter
        .convertFunction(BuiltInFunctionDefinitions.GREATEST.getName(),BuiltInFunctionDefinitions.GREATEST.getName(), BuiltInFunctionDefinitions.GREATEST);
    LEAST = converter
        .convertFunction(BuiltInFunctionDefinitions.LEAST.getName(),BuiltInFunctionDefinitions.LEAST.getName(), BuiltInFunctionDefinitions.LEAST);
    NOW = converter
        .convertFunction("NOW",
            "NOW", TimeFunctions.NOW);
  }

  public Map<String, SqlFunction> getDefaultFunctions() {

    return Map.of(
        COALESCE.getName(), COALESCE,
        GREATEST.getName(), GREATEST,
        LEAST.getName(), LEAST,
        NOW.getName(), NOW);
  }
}
