package com.datasqrl;

import com.datasqrl.calcite.type.TypeFactory;
import com.datasqrl.flink.FlinkConverter;
import com.datasqrl.function.SqrlFunction;
import java.time.Instant;
import java.util.Map;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlFunction;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.ScalarFunction;

public class DefaultFunctions {
  public static final FlinkConverter converter = new FlinkConverter(new TypeFactory());
  public static SqlFunction COALESCE;
  public static SqlFunction GREATEST;
  public static SqlFunction LEAST;
  public static SqlFunction NOW;
  static {
    COALESCE = converter
        .convertFunction(BuiltInFunctionDefinitions.COALESCE.getName(), BuiltInFunctionDefinitions.COALESCE)
        .get();
    GREATEST = converter
        .convertFunction(BuiltInFunctionDefinitions.GREATEST.getName(), BuiltInFunctionDefinitions.GREATEST)
        .get();
    LEAST = converter
        .convertFunction(BuiltInFunctionDefinitions.LEAST.getName(), BuiltInFunctionDefinitions.LEAST)
        .get();
    NOW = converter
        .convertFunction("NOW", new Now())
        .get();
  }

  public static class Now extends ScalarFunction implements SqrlFunction {

    public Now() {
    }

    public Instant eval() {
      return Instant.now();
    }

    @Override
    public String getDocumentation() {
      return "";
    }
  }

  public Map<String, SqlFunction> getDefaultFunctions() {
    return Map.of(
        COALESCE.getName(), COALESCE,
        GREATEST.getName(), GREATEST,
        LEAST.getName(), LEAST,
        NOW.getName(), NOW);
  }
}
