package com.datasqrl.function;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.calcite.type.TypeFactory;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.flink.FlinkConverter;
import com.datasqrl.module.FunctionNamespaceObject;
import com.datasqrl.canonicalizer.Name;
import java.net.URL;
import java.util.Optional;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlFunction;
import org.apache.flink.table.functions.FunctionDefinition;

@Value
@Slf4j
public class FlinkUdfNsObject implements FunctionNamespaceObject<FunctionDefinition> {
  Name name;
  FunctionDefinition function;
  Optional<URL> jarUrl;

  public FlinkUdfNsObject(String name, FunctionDefinition function, Optional<URL> jarUrl) {
    this.name = Name.system(name);
    this.function = function;
    this.jarUrl = jarUrl;
  }

  @Override
  public boolean apply(Optional<String> objectName, SqrlFramework framework, ErrorCollector errors) {
    FlinkConverter flinkConverter = new FlinkConverter((TypeFactory) framework.getQueryPlanner().getCatalogReader()
        .getTypeFactory());

    String name = objectName.orElseGet(() -> getFunctionName(function));

    Optional<SqlFunction> convertedFunction = flinkConverter
        .convertFunction(name, function);

    if (convertedFunction.isEmpty()) {
      log.info("Could not resolve function: " + name);
      return false;
    }

    framework.getSchema()
        .addFunction(name, convertedFunction.get());

    jarUrl.ifPresent((url)->framework.getSchema().addJar(url));
    return true;
  }

  public static String getFunctionName(FunctionDefinition function) {
    return getFunctionNameFromClass(function.getClass()).getDisplay();
  }
  public static Name getFunctionNameFromClass(Class clazz) {
    String fctName = clazz.getSimpleName();
    fctName = Character.toLowerCase(fctName.charAt(0)) + fctName.substring(1);
    return Name.system(fctName);
  }
}
