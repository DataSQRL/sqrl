package com.datasqrl.loaders;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.module.NamespaceObject;
import com.datasqrl.plan.MainScript;
import com.datasqrl.plan.validate.ScriptPlanner;
import java.nio.file.Path;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class ScriptNamespaceObject implements NamespaceObject {
  ModuleLoader moduleLoader;
  Name name;
  String script;

  @Override
  public boolean apply(ScriptPlanner planner, Optional<String> objectName, SqrlFramework framework, ErrorCollector errors) {

    planner.plan(
        new MainScript() {

          @Override
          public Optional<Path> getPath() {
            return Optional.empty();
          }

          @Override
          public String getContent() {
            return script;
          }
        }, moduleLoader);
    return true;
  }

}
