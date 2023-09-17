package com.datasqrl.function;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.flink.FlinkConverter;
import com.datasqrl.module.FunctionNamespaceObject;
import com.datasqrl.canonicalizer.Name;
import java.net.URL;
import java.util.Optional;
import lombok.Value;
import org.apache.calcite.sql.SqlFunction;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.UserDefinedFunction;

@Value
public class FlinkUdfNsObject implements FunctionNamespaceObject<FunctionDefinition> {
  Name name;
  FunctionDefinition function;
  Optional<URL> jarUrl;

  @Override
  public boolean apply(Optional<String> objectName, SqrlFramework framework, ErrorCollector errors) {
    FlinkConverter flinkConverter = new FlinkConverter(framework.getQueryPlanner().getRexBuilder(),
        framework.getQueryPlanner().getTypeFactory());

    SqlFunction convertedFunction = flinkConverter
        .convertFunction(objectName.orElse(((SqrlFunction) function).getFunctionName().getDisplay()),
            ((SqrlFunction) function).getFunctionName().getDisplay(), function);

    framework.getSqrlOperatorTable()
        .addFunction(objectName.orElse(name.getDisplay()), convertedFunction);

    jarUrl.ifPresent((url)->framework.getSchema().addJar(url));
    return true;
  }
}
