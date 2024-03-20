package com.datasqrl;

import static com.datasqrl.util.FunctionUtil.getFunctionByNameFromClass;

import com.datasqrl.calcite.type.TypeFactory;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.flink.FlinkConverter;
import com.datasqrl.function.CalciteFunctionNsObject;
import com.datasqrl.function.FlinkUdfNsObject;
import com.datasqrl.function.SqrlFunction;
import com.datasqrl.util.FunctionUtil;
import com.datasqrl.module.NamespaceObject;
import com.google.common.base.Preconditions;
import java.util.Locale;
import java.util.Optional;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlFunction;
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
    FlinkConverter converter = new FlinkConverter(TypeFactory.getTypeFactory());

    Optional<BuiltInFunctionDefinition> function = getFunctionByNameFromClass(BuiltInFunctionDefinitions.class,
        BuiltInFunctionDefinition.class,
        originalName.toUpperCase(Locale.ROOT));
    Preconditions.checkArgument(function.isPresent(), "Could not find function %s", name);
    BuiltInFunctionDefinition fnc = function.get();
    SqlFunction sqlFunction = converter.convertFunction(originalName, fnc).get();

    return new CalciteFunctionNsObject(Name.system(name), sqlFunction, originalName);
  }

  public static NamespaceObject createFunctionFromStdOpTable(String name) {
    return new CalciteFunctionNsObject(Name.system(name),
        getFunctionByNameFromClass(SqlStdOperatorTable.class,
            SqlOperator.class, name).get(), name);
  }

  public static NamespaceObject createNsObject(FunctionDefinition function) {
    Preconditions.checkArgument(function instanceof FunctionDefinition,
        "All SQRL function implementations must extend FunctionDefinition: %s", function.getClass());
    return new FlinkUdfNsObject(getFunctionNameFromClass(function.getClass()), function, Optional.empty());
  }

  static String getFunctionNameFromClass(Class clazz) {
    String fctName = clazz.getSimpleName();
    fctName = Character.toLowerCase(fctName.charAt(0)) + fctName.substring(1);
    return fctName;
  }
}
