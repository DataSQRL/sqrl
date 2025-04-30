package com.datasqrl.function;

import java.net.URISyntaxException;
import java.net.URL;
import java.util.List;
import java.util.Optional;

import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.resource.ResourceType;
import org.apache.flink.table.resource.ResourceUri;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.module.FunctionNamespaceObject;

import lombok.SneakyThrows;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

@Value
@Slf4j
public class FlinkUdfNsObject implements FunctionNamespaceObject<FunctionDefinition> {
  Name name;
  FunctionDefinition function;
  private final String sqlName;
  Optional<URL> jarUrl;

  public FlinkUdfNsObject(String name, FunctionDefinition function, String sqlName,
      Optional<URL> jarUrl) {
    this.name = Name.system(name);
    this.function = function;
    this.sqlName = sqlName;
    this.jarUrl = jarUrl;
  }
 
  public static String getFunctionName(FunctionDefinition function) {
    if (function instanceof BuiltInFunctionDefinition) {
      return ((BuiltInFunctionDefinition) function).getName();
    }

    return getFunctionNameFromClass(function.getClass()).getDisplay();
  }
  public static Name getFunctionNameFromClass(Class clazz) {
    var fctName = clazz.getSimpleName();
    fctName = Character.toLowerCase(fctName.charAt(0)) + fctName.substring(1);
    return Name.system(fctName);
  }
}
