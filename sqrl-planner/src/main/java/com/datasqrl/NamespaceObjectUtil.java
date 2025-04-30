package com.datasqrl;

import java.util.Locale;
import java.util.Optional;

import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.function.FlinkUdfNsObject;
import com.datasqrl.module.NamespaceObject;
import com.google.common.base.Preconditions;

public class NamespaceObjectUtil {

  public static FlinkUdfNsObject createNsObject(FunctionDefinition function) {
    Preconditions.checkArgument(function instanceof FunctionDefinition,
        "All SQRL function implementations must extend FunctionDefinition: %s", function.getClass());
    var functionNameFromClass = getFunctionNameFromClass(function.getClass());
    return new FlinkUdfNsObject(functionNameFromClass, function, functionNameFromClass, Optional.empty());
  }

  static String getFunctionNameFromClass(Class clazz) {
    var fctName = clazz.getSimpleName();
    fctName = Character.toLowerCase(fctName.charAt(0)) + fctName.substring(1);
    return fctName;
  }
}
