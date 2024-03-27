package com.datasqrl;

import static org.apache.flink.table.functions.FunctionKind.SCALAR;

import com.datasqrl.calcite.type.TypeFactory;
import com.datasqrl.flink.FlinkConverter;
import com.datasqrl.function.DocumentedFunction;
import java.time.Instant;
import java.util.Map;
import org.apache.calcite.sql.SqlFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.inference.TypeStrategies;

public class DefaultFunctions {
  public static final FlinkConverter converter = new FlinkConverter(new TypeFactory());
  public static SqlFunction COALESCE;
  public static SqlFunction GREATEST;
  public static SqlFunction LEAST;
  public static SqlFunction PROCTIME;
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
    PROCTIME = converter
        .convertFunction(BuiltInFunctionDefinitions.PROCTIME.getName(),
            //Redefine proctime to be a scalar to conform to other flink sql. We'll drop this fnc later.
            BuiltInFunctionDefinition.newBuilder()
                .name("proctime")
                .kind(SCALAR)
                .outputTypeStrategy(TypeStrategies.explicit(
                    DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3).notNull()))
                .build())
        .get();
    NOW = converter
        .convertFunction("NOW", new Now())
        .get();
  }

  public static class Now extends ScalarFunction implements DocumentedFunction {

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
        PROCTIME.getName(), PROCTIME,
        NOW.getName(), NOW);
  }
}
