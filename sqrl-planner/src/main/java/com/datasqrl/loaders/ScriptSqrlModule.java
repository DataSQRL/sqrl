package com.datasqrl.loaders;

import static com.datasqrl.canonicalizer.Name.HIDDEN_PREFIX;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.calcite.function.SqrlTableMacro;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.config.PackageJson;
import com.datasqrl.engine.log.LogManager;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.module.NamespaceObject;
import com.datasqrl.module.SqrlModule;
import com.datasqrl.plan.MainScript;
import com.datasqrl.plan.validate.ScriptPlanner;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.AllArgsConstructor;
import org.apache.calcite.jdbc.CalciteSchema.FunctionEntry;
import org.apache.calcite.jdbc.CalciteSchema.FunctionEntryImpl;
import org.apache.calcite.jdbc.SqrlSchema;
import org.apache.calcite.schema.Function;

@AllArgsConstructor
public class ScriptSqrlModule implements SqrlModule {

  private final ModuleLoader moduleLoader;
  private final String script;
  private Optional<SqrlSchema> schema;
  private PackageJson sqrlConfig;
  private LogManager logManager;

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

    SqrlSchema schema = new SqrlSchema(framework.getTypeFactory(), framework.getNameCanonicalizer());
    this.schema = Optional.of(schema);

    SqrlFramework newFramework = new SqrlFramework(framework.getRelMetadataProvider(),
        framework.getHintStrategyTable(), framework.getNameCanonicalizer(), schema,
        Optional.of(framework.getQueryPlanner()));
    ScriptPlanner newPlanner = new ScriptPlanner(newFramework, moduleLoader, errors,
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
      if (!planned.get()) {
        planned.set(true);
        plan(planner, objectName, framework, errors);
      }

        //Add all non-sqrl tables to current schema (e.g. no table mappings)
      SqrlSchema curSchema = framework.getSchema();
      SqrlSchema scriptSchema = schema.get();
      if (tableName.isEmpty()) {
        curSchema.getPathToAbsolutePathMap().putAll(scriptSchema.getPathToAbsolutePathMap());
        curSchema.getPathToSysTableMap().putAll(scriptSchema.getPathToSysTableMap());

        curSchema.getUdf().putAll(scriptSchema.getUdf());
        curSchema.getUdfListMap().putAll(scriptSchema.getUdfListMap());
        curSchema.getTableNameToIdMap().putAll(scriptSchema.getTableNameToIdMap());
        curSchema.getAddlSql().addAll(scriptSchema.getAddlSql());

        for (Map.Entry<String, List<FunctionEntry>> entry : scriptSchema.getFunctionMap().map().entrySet()) {
          if (!entry.getKey().startsWith(HIDDEN_PREFIX)) {
            for (FunctionEntry entry1 : entry.getValue()) {
              curSchema.getFunctionMap().put(entry.getKey(), entry1);
            }
          }
        }

      } else {
        Name table = tableName.get();
        Name destName = objectName.map(o -> Name.system(o)).orElse(tableName.get());
        curSchema.getPathToAbsolutePathMap().put(
            destName.toNamePath(), scriptSchema.getPathToAbsolutePathMap().get(table.toNamePath()));
        curSchema.getPathToSysTableMap().put(
            destName.toNamePath(),
            scriptSchema.getPathToSysTableMap().get(table.toNamePath()));

        curSchema.getUdf().putAll(scriptSchema.getUdf());
        curSchema.getUdfListMap().putAll(scriptSchema.getUdfListMap());
        curSchema.getTableNameToIdMap().putAll(scriptSchema.getTableNameToIdMap());
        curSchema.getAddlSql().addAll(scriptSchema.getAddlSql());

        for (Map.Entry<String, List<FunctionEntry>> entry : scriptSchema.getFunctionMap().map().entrySet()) {
          if (entry.getKey().equalsIgnoreCase(table.getCanonical())) {
            for (FunctionEntry entry1 : entry.getValue()) {
              Function function = entry1.getFunction();
              if (function instanceof SqrlTableMacro) {
                SqrlTableMacro sqrlTableMacro = (SqrlTableMacro) function;
                SqrlTableMacro renamed = sqrlTableMacro.rename(destName);

                curSchema.getFunctionMap().put(destName.getDisplay(),
                    new FunctionEntryImpl(curSchema, destName.getDisplay(), renamed));
              }

            }
          }
        }
      }


      return true;
    }
  }
}
