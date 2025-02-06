package com.datasqrl.loaders;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.calcite.jdbc.SqrlSchema;

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

import lombok.AllArgsConstructor;

@AllArgsConstructor
public class ScriptSqrlModule implements SqrlModule {

  private final ModuleLoader moduleLoader;
  private final String script;
  private Optional<SqrlSchema> schema;
  private PackageJson sqrlConfig;
  private LogManager logManager;
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

  protected void plan(ScriptPlanner planner, Optional<String> objectName, SqrlFramework framework,
      ErrorCollector errors) {
    // Construct a new schema (et al) and plan script. store schema here. When importing a name, pull from schema

    var schema = new SqrlSchema(framework.getTypeFactory(), framework.getNameCanonicalizer());
    this.schema = Optional.of(schema);

    var newFramework = new SqrlFramework(framework.getRelMetadataProvider(),
        framework.getHintStrategyTable(), framework.getNameCanonicalizer(), schema,
        Optional.of(framework.getQueryPlanner()));
    var newPlanner = new ScriptPlanner(newFramework, moduleLoader, errors,
        planner.getExecutionGoal(), planner.getTableFactory(), sqrlConfig, planner.getNameUtil(),
        planner.getConnectorFactoryFactory(), logManager, planner.getCreateTableResolver());

    newPlanner.plan(
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

    schema.getTableNames().stream()
        .map(t->schema.getTable(t, false))
        .filter(t-> !(t.getTable() instanceof SqrlTableMacro))
        .forEach(t->framework.getSchema().add(t.name, t.getTable()));
  }

  @AllArgsConstructor
  public class ScriptNamespaceObject implements NamespaceObject {

    Optional<Name> tableName;//specific table name to import, empty for all

    @Override
    public Name getName() {
      throw new RuntimeException("not implemented");
    }

    @Override
    public boolean apply(ScriptPlanner planner, Optional<String> objectName,
        SqrlFramework framework, ErrorCollector errors) {
        //Add all non-sqrl tables to current schema (e.g. no table mappings)
      if (tableName.isEmpty()) {
        //Plan over current schema
        if (!planned.get()) {
          planned.set(true);
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
        }
      } else {
        throw new RuntimeException(
            String.format("Importing specific tables are not currently supported, please import all: IMPORT %s.*;",
                namePath.popLast().toString()));
      }

      return true;
    }
  }
}
