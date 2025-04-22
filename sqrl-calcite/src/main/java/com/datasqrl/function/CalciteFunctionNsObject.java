package com.datasqrl.function;

import java.net.URL;
import java.util.Optional;

import org.apache.calcite.sql.SqlOperator;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.module.FunctionNamespaceObject;
import com.datasqrl.plan.validate.ScriptPlanner;

import lombok.Value;

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
    var objName = objectName.orElse(name.getDisplay());
    framework.getSchema().addFunctionAlias(objName, this.sqlName);
    return true;
  }
}
