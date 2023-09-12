package com.datasqrl.functions;

import com.datasqrl.flink.FlinkConverter;
import java.util.Map;
import lombok.AllArgsConstructor;
import org.apache.calcite.sql.SqlFunction;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;

@AllArgsConstructor
public class DefaultFunctions {
  FlinkConverter converter;

  public Map<String, SqlFunction> getDefaultFunctions() {
    SqlFunction coalesce = converter
        .convertFunction(BuiltInFunctionDefinitions.COALESCE.getName(), BuiltInFunctionDefinitions.COALESCE.getName(), BuiltInFunctionDefinitions.COALESCE);
    SqlFunction greatest = converter
        .convertFunction(BuiltInFunctionDefinitions.GREATEST.getName(),BuiltInFunctionDefinitions.GREATEST.getName(), BuiltInFunctionDefinitions.GREATEST);
    SqlFunction least = converter
        .convertFunction(BuiltInFunctionDefinitions.LEAST.getName(),BuiltInFunctionDefinitions.LEAST.getName(), BuiltInFunctionDefinitions.LEAST);
    SqlFunction now = converter
        .convertFunction(TimeFunctions.NOW.getFunctionName().getCanonical(),
            TimeFunctions.NOW.getFunctionName().getCanonical(), TimeFunctions.NOW);

    return Map.of(
        coalesce.getName(), coalesce,
        greatest.getName(), greatest,
        least.getName(), least,
        now.getName(), now);
  }
}
