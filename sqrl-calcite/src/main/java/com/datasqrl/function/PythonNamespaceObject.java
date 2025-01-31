package com.datasqrl.function;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.module.NamespaceObject;
import com.datasqrl.plan.validate.ScriptPlanner;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.calcite.jdbc.SqrlSchema.PythonUdf;
import org.apache.flink.table.catalog.ContextResolvedFunction;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.functions.python.PythonFunction;

@AllArgsConstructor
@Getter
public class PythonNamespaceObject implements NamespaceObject {

  PythonFunction pythonFunction;
  Name name;
  String path;
  NamePath directory;

  @Override
  public boolean apply(ScriptPlanner planner, Optional<String> alias, SqrlFramework framework,
      ErrorCollector errors) {
    UserDefinedFunction udf = (UserDefinedFunction) pythonFunction;
    framework.getFlinkFunctionCatalog()
        .registerTemporarySystemFunction(getName().getDisplay(), udf, true);

//    framework.getSchema().getUdf().put(getName().getDisplay(), udf);
    framework.getSchema().getPythonUdfs()
        .add(new PythonUdf(directory, name, path));
    return true;
  }
}
