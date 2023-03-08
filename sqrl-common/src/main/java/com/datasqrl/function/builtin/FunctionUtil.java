package com.datasqrl.function.builtin;

import com.datasqrl.function.SqrlFunction;
import com.datasqrl.name.Name;
import com.datasqrl.plan.local.generate.CalciteFunctionNsObject;
import com.datasqrl.plan.local.generate.FlinkUdfNsObject;
import com.datasqrl.plan.local.generate.NamespaceObject;
import com.google.common.base.Preconditions;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Optional;
import org.apache.calcite.sql.SqlFunction;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;

public class FunctionUtil {

  public static NamespaceObject createNsObject(String name, SqlFunction fnc) {
    return new CalciteFunctionNsObject(Name.system(name), fnc, "");
  }

  public static NamespaceObject createFunctionFromFlink(String name) {
    return createFunctionFromFlink(name, name);
  }

  public static NamespaceObject createFunctionFromFlink(String name, String originalName) {
    Optional<SqlFunction> function = getFunctionByNameFromClass(FlinkSqlOperatorTable.class, originalName);
    Preconditions.checkArgument(function.isPresent());
    return new CalciteFunctionNsObject(Name.system(name), function.get(), originalName);
  }

  private static Optional<SqlFunction> getFunctionByNameFromClass(Class clazz, String name) {
    for (Field field : clazz.getDeclaredFields()) {
      try {
        if (Modifier.isStatic(field.getModifiers()) && SqlFunction.class.isAssignableFrom(field.getType())) {
          if (field.getName().equalsIgnoreCase(name)) {
            return Optional.of((SqlFunction)field.get(null));
          }
        }
      } catch (IllegalAccessException e) {

      }
    }
    return Optional.empty();
  }


  public static NamespaceObject createNsObject(SqrlFunction function) {
    Preconditions.checkArgument(function instanceof ScalarFunction,
        "All SQRL function implementations must extend ScalarFunction: %s", function.getClass());
    return new FlinkUdfNsObject(function.getFunctionName(), (ScalarFunction)function, Optional.empty());
  }

  public static Name getFunctionNameFromClass(Class clazz) {
    String fctName = clazz.getSimpleName();
    fctName = Character.toLowerCase(fctName.charAt(0)) + fctName.substring(1);
    return Name.system(fctName);
  }

}
