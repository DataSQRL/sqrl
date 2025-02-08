package com.datasqrl.loaders;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.calcite.function.SqrlTableMacro;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.config.PackageJson;
import com.datasqrl.engine.log.LogManager;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.module.NamespaceObject;
import com.datasqrl.module.SqrlModule;
import com.datasqrl.plan.MainScript;
import com.datasqrl.plan.validate.ScriptPlanner;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.calcite.jdbc.SqrlSchema;

@AllArgsConstructor
public class ScriptSqrlModule implements SqrlModule {

  private final String scriptContent;
  private final Path scriptPath;
  private final NamePath namePath;

  private final AtomicBoolean planned = new AtomicBoolean(false);

  //Planning a single table. Return a planning ns object and then only add that table
  @Override
  public Optional<NamespaceObject> getNamespaceObject(Name name) {
    return Optional.of(new ScriptNamespaceObject(Optional.of(name)));
  }

  //Planning all tables. Return a single planning ns object and add all tables
  @Override
  public List<NamespaceObject> getNamespaceObjects() {
    return List.of(new ScriptNamespaceObject(Optional.empty()));
  }

  @AllArgsConstructor
  public class ScriptNamespaceObject implements NamespaceObject {

    @Getter
    Optional<Name> tableName;//specific table name to import, empty for all

    @Override
    public Name getName() {
      return namePath.getLast();
    }

    public MainScript getScript() {
      return new MainScript() {
        @Override
        public Optional<Path> getPath() {
          return Optional.of(scriptPath);
        }

        @Override
        public String getContent() {
          return scriptContent;
        }
      };
    }

    @Override
    public boolean apply(ScriptPlanner planner, Optional<String> objectName,
        SqrlFramework framework, ErrorCollector errors) {
      throw new UnsupportedOperationException();
    }
  }
}
