package com.datasqrl.function;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.module.FunctionNamespaceObject;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.plan.validate.ScriptPlanner;
import java.net.URL;
import java.util.Optional;
import lombok.Value;
import org.apache.calcite.sql.SqlOperator;

@Value
public class CalciteFunctionNsObject implements FunctionNamespaceObject<SqlOperator> {
  Name name;
  SqlOperator function;

  String sqlName;

  @Override
  public Optional<URL> getJarUrl() {
    return Optional.empty();
  }

  @Override
  public boolean apply(ScriptPlanner planner, Optional<String> objectName, SqrlFramework framework, ErrorCollector errors) {
    framework.getSchema()
        .addFunction(objectName.orElse(name.getDisplay()), function);
    return true;
  }
}
