package com.datasqrl.calcite.sql;

import com.datasqrl.function.SqrlFunctionParameter;
import java.util.List;
import lombok.AllArgsConstructor;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;

// Uses ':name' to represent dynamic parameters
@AllArgsConstructor
public class OrmDynamicParameterStrategy implements DynamicParameterStrategy {
  List<SqrlFunctionParameter> parameterList;

  @Override
  public void apply(SqlPrettyWriter sqlWriter, int index) {
    sqlWriter.print(":" + parameterList.get(index).getVariableName());
  }
}
