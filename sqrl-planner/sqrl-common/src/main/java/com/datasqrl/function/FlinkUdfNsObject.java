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
import org.apache.calcite.sql.SqlFunction;
import org.apache.flink.table.functions.FunctionDefinition;

@Value
public class FlinkUdfNsObject implements FunctionNamespaceObject<FunctionDefinition> {
  Name name;
  FunctionDefinition function;
  Optional<URL> jarUrl;

  @Override
  public boolean apply(Optional<String> objectName, SqrlFramework framework, ErrorCollector errors) {
    FlinkConverter flinkConverter = new FlinkConverter((TypeFactory) framework.getQueryPlanner().getCatalogReader()
        .getTypeFactory());

    SqlFunction convertedFunction = flinkConverter
        .convertFunction(
            ((SqrlFunction) function).getFunctionName().getDisplay(), function);

    framework.getSqrlOperatorTable()
        .addFunction(objectName.orElse(name.getDisplay()), convertedFunction);

    jarUrl.ifPresent((url)->framework.getSchema().addJar(url));
    return true;
  }
}
