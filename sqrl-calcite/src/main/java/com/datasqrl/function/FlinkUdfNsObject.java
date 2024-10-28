package com.datasqrl.function;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.error.ErrorCollector;
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
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.resource.ResourceType;
import org.apache.flink.table.resource.ResourceUri;

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

  @SneakyThrows
  @Override
  public boolean apply(ScriptPlanner planner, Optional<String> objectName, SqrlFramework framework, ErrorCollector errors) {
    String name = objectName.orElseGet(() -> getThisFunctionName(function));

    jarUrl.ifPresent(url-> {
      try {
        framework.getFlinkFunctionCatalog()
            .registerFunctionJarResources(name, List.of(new ResourceUri(ResourceType.JAR, url.toURI().toString())));
      } catch (URISyntaxException e) {
        throw new RuntimeException(e);
      }
    });

    if (function instanceof UserDefinedFunction) {
      UserDefinedFunction udf = (UserDefinedFunction)function;
      framework.getFlinkFunctionCatalog()
          .registerCatalogFunction(UnresolvedIdentifier.of(name), udf.getClass(), true);
      framework.getSchema().getUdf().put(name, udf);
    } else if (function instanceof BuiltInFunctionDefinition) {
      //if name is different from function name, create function alias
      framework.getSchema().addFunctionAlias(name.toLowerCase(), sqlName.toLowerCase());
    }

    jarUrl.ifPresent((url)->framework.getSchema().addJar(url));
    return true;
  }
  public String getThisFunctionName(FunctionDefinition function) {
    if (function instanceof BuiltInFunctionDefinition) {
      return this.name.getDisplay();
    }

    return getFunctionNameFromClass(function.getClass()).getDisplay();
  }

  public static String getFunctionName(FunctionDefinition function) {
    if (function instanceof BuiltInFunctionDefinition) {
      return ((BuiltInFunctionDefinition) function).getName();
    }

    return getFunctionNameFromClass(function.getClass()).getDisplay();
  }
  public static Name getFunctionNameFromClass(Class clazz) {
    String fctName = clazz.getSimpleName();
    fctName = Character.toLowerCase(fctName.charAt(0)) + fctName.substring(1);
    return Name.system(fctName);
  }
}
