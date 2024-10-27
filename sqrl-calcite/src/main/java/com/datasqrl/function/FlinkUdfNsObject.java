package com.datasqrl.function;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.calcite.type.TypeFactory;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.flink.FlinkConverter;
import com.datasqrl.module.FunctionNamespaceObject;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.plan.validate.ScriptPlanner;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.List;
import java.util.Optional;
import lombok.SneakyThrows;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlFunction;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.resource.ResourceType;
import org.apache.flink.table.resource.ResourceUri;

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

  @SneakyThrows
  @Override
  public boolean apply(ScriptPlanner planner, Optional<String> objectName, SqrlFramework framework, ErrorCollector errors) {
    String name = objectName.orElseGet(() -> getFunctionName(function));

    UserDefinedFunction udf = (UserDefinedFunction) function;
    jarUrl.ifPresent(url-> {
      try {
        framework.getFlinkFunctionCatalog()
            .registerFunctionJarResources(name, List.of(new ResourceUri(ResourceType.JAR, url.toURI().toString())));
      } catch (URISyntaxException e) {
        throw new RuntimeException(e);
      }
    });

    //TODO: Remove all below and only preserve jar location
    framework.getFlinkFunctionCatalog()
        .registerCatalogFunction(UnresolvedIdentifier.of(name), udf.getClass(), true);

    FlinkConverter flinkConverter = new FlinkConverter((TypeFactory) framework.getQueryPlanner().getCatalogReader()
        .getTypeFactory());


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
