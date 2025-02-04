package com.datasqrl;

import static com.datasqrl.util.FunctionUtil.getFunctionByNameFromClass;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.function.CalciteFunctionNsObject;
import com.datasqrl.function.FlinkUdfNsObject;
import com.datasqrl.module.NamespaceObject;
import com.google.common.base.Preconditions;
import java.util.Locale;
import java.util.Optional;

import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;

public class NamespaceObjectUtil {

  public static NamespaceObject createFunctionFromFlink(String name) {
    return createFunctionFromFlink(name, name);
  }

  public static NamespaceObject createFunctionFromFlink(String name, String originalName) {
    Optional<BuiltInFunctionDefinition> function = getFunctionByNameFromClass(BuiltInFunctionDefinitions.class,
        BuiltInFunctionDefinition.class,
        originalName.toUpperCase(Locale.ROOT));
    Preconditions.checkArgument(function.isPresent(), "Could not find function %s", name);
    BuiltInFunctionDefinition fnc = function.get();
    return new FlinkUdfNsObject(name, fnc, originalName, Optional.empty());
  }

  public static NamespaceObject createFunctionFromStdOpTable(String name) {
    return new CalciteFunctionNsObject(Name.system(name),
        getFunctionByNameFromClass(SqlStdOperatorTable.class,
            SqlOperator.class, name).get(), name);
  }

  public static FlinkUdfNsObject createNsObject(FunctionDefinition function) {
    Preconditions.checkArgument(function instanceof FunctionDefinition,
        "All SQRL function implementations must extend FunctionDefinition: %s", function.getClass());
    String functionNameFromClass = getFunctionNameFromClass(function.getClass());
    return new FlinkUdfNsObject(functionNameFromClass, function, functionNameFromClass, Optional.empty());
  }

  static String getFunctionNameFromClass(Class clazz) {
    String fctName = clazz.getSimpleName();
    fctName = Character.toLowerCase(fctName.charAt(0)) + fctName.substring(1);
    return fctName;
  }
}
