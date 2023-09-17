package com.datasqrl;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.function.CalciteFunctionNsObject;
import com.datasqrl.function.FlinkUdfNsObject;
import com.datasqrl.function.SqrlFunction;
import com.datasqrl.util.FunctionUtil;
import com.datasqrl.module.NamespaceObject;
import com.google.common.base.Preconditions;
import java.util.Optional;
import org.apache.calcite.sql.SqlFunction;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;

public class NamespaceObjectUtil {

  public static NamespaceObject createNsObject(String name, SqlFunction fnc) {
    return new CalciteFunctionNsObject(Name.system(name), fnc, "");
  }

  public static NamespaceObject createFunctionFromFlink(String name) {
    return createFunctionFromFlink(name, name);
  }

  public static NamespaceObject createFunctionFromFlink(String name, String originalName) {
    Optional<SqlFunction> function = FunctionUtil.getFunctionByNameFromClass(FlinkSqlOperatorTable.class, originalName);
    Preconditions.checkArgument(function.isPresent());
    return new CalciteFunctionNsObject(Name.system(name), function.get(), originalName);
  }


  public static NamespaceObject createNsObject(SqrlFunction function) {
    Preconditions.checkArgument(function instanceof FunctionDefinition,
        "All SQRL function implementations must extend FunctionDefinition: %s", function.getClass());
    return new FlinkUdfNsObject(function.getFunctionName(), (FunctionDefinition)function, Optional.empty());
  }

}
