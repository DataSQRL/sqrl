package com.datasqrl.graphql;

import com.datasqrl.calcite.function.SqrlTableMacro;
import com.datasqrl.calcite.type.NamedRelDataType;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.config.LogEngineSupplier;
import com.datasqrl.engine.log.Log;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.tables.TableSink;
import com.datasqrl.io.tables.TableSource;
import com.datasqrl.loaders.ModuleLoader;
import com.datasqrl.loaders.ModuleLoaderStd;
import com.datasqrl.loaders.TableSourceSinkNamespaceObject;
import com.datasqrl.module.NamespaceObject;
import com.datasqrl.module.SqrlModule;
import com.datasqrl.plan.queries.APIMutation;
import com.datasqrl.plan.queries.APIQuery;
import com.datasqrl.plan.queries.APISource;
import com.datasqrl.plan.queries.APISubscription;
import com.datasqrl.plan.table.CalciteTableFactory;
import com.datasqrl.plan.table.PhysicalRelationalTable;
import com.datasqrl.plan.table.QueryRelationalTable;
import com.datasqrl.plan.table.ScriptRelationalTable;
import com.datasqrl.plan.table.TableType;
import com.datasqrl.schema.RootSqrlTable;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.calcite.jdbc.SqrlSchema;
import org.apache.calcite.rel.type.RelDataTypeFactory;

@Getter
@Singleton
@AllArgsConstructor(onConstructor_=@Inject)
public class APIConnectorManagerImpl implements APIConnectorManager {

  private final CalciteTableFactory tableFactory;
  private final LogEngineSupplier logEngine;
  private final ErrorCollector errors;
  private final ModuleLoader moduleLoader;
  private final RelDataTypeFactory typeFactory;

  private final SqrlSchema sqrlSchema;

  /**
   * Adds mutation by connecting it to a table source and sink.
   * Those are either loaded if the module for the api source exists or created by the log engine.
   *
   * @param mutation
   */
  @Override
  public void addMutation(APIMutation mutation) {
    NamePath apiNamePath = apiToModulePath(mutation.getSource());
    Optional<SqrlModule> module = moduleLoader.getModule(apiNamePath);
    if (module.isPresent()) {
      Optional<NamespaceObject> log = module.get().getNamespaceObject(mutation.getName());
      errors.checkFatal(log.isPresent(), "Could not load mutation endpoint for %s from module %s",
          mutation, module.get());
      errors.checkFatal(log.get() instanceof TableSourceSinkNamespaceObject, "Loaded mutation endpoint for %s from module %s is not a source and sink",
          mutation, module.get());
      TableSourceSinkNamespaceObject sourceSink = (TableSourceSinkNamespaceObject) log.get();
      sqrlSchema.getMutations().put(mutation, sourceSink.getSink());
    } else {
      //Create module if log engine is set
      errors.checkFatal(logEngine.isPresent(), "Cannot create mutation %s: Could not load "
          + "module for %s and no log engine configured", mutation, apiNamePath);
      SqrlModule logModule = sqrlSchema.getModules().get(apiNamePath);
      if (logModule==null) {
        logModule = new LogModule();
        sqrlSchema.getModules().put(apiNamePath, logModule);
      }
      String logId = getLogId(mutation);
      Log log = logEngine.get().createLog(logId, mutation.getSchema());
      ((LogModule)logModule).addEntry(mutation.getName(),log);
      sqrlSchema.getMutations().put(mutation, log.getSink());
    }
  }

  @Override
  public TableSink getMutationSource(APISource source, Name mutationName) {
    return sqrlSchema.getMutations().get(new APIMutation(mutationName, source, null));
  }

  @Override
  public TableSource addSubscription(APISubscription subscription, SqrlTableMacro sqrlTable) {
    errors.checkFatal(logEngine.isPresent(), "Cannot create subscriptions because no log engine is configured");
    RootSqrlTable rootSqrlTable = (RootSqrlTable) sqrlTable;
    PhysicalRelationalTable table = ((PhysicalRelationalTable) rootSqrlTable.getInternalTable());
    errors.checkFatal(table.getRoot().getType() == TableType.STREAM,
        "Table %s for subscription %s is not a stream table", table.getTableName(), subscription.getName());
    //Check if we already exported it
    TableSource subscriptionSource;
    if (sqrlSchema.getApiExports().containsKey(sqrlTable)) {
      subscriptionSource = ((Log)sqrlSchema.getApiExports().get(sqrlTable)).getSource();
    } else {
      //otherwise create new log for it
      String logId = table.getNameId();
      NamedRelDataType tableSchema = new NamedRelDataType(table.getTableName(),
          table.getRowType());
      Log log = logEngine.get().createLog(logId, tableSchema);
      sqrlSchema.getApiExports().put(sqrlTable, log);
      subscriptionSource = log.getSource();
    }
    sqrlSchema.getSubscriptions().put(subscription, subscriptionSource);
    return subscriptionSource;
  }

  @Override
  public void addQuery(APIQuery query) {
    sqrlSchema.getQueries().add(query);
  }

  @Override
  public void updateModuleLoader(ModuleLoader moduleLoader) {
  }

  @Override
  public ModuleLoader getModuleLoader() {
    return new ModuleLoaderStd(sqrlSchema.getModules());
  }

  @Override
  public List<Log> getLogs() {

    List<Log> logs = new ArrayList<>();
    logs.addAll((Collection) sqrlSchema.getApiExports().values());
    sqrlSchema.getModules().values().stream()
        .flatMap(logModule -> ((LogModule)logModule).entries.values().stream())
        .forEach(logs::add);
    return logs;
  }

  @Override
  public List<APIQuery> getQueries() {
    return sqrlSchema.getQueries();
  }

  @Override
  public Map<SqrlTableMacro, Log> getExports() {
    return (Map)sqrlSchema.getApiExports();
  }

  private NamePath apiToModulePath(APISource source) {
    return source.getName().toNamePath();
  }

  public static String getLogId(APIMutation mutation) {
    return mutation.getSource().getName().getCanonical() + "-" + mutation.getName().getCanonical();
  }

  public class LogModule implements SqrlModule {

    Map<Name, Log> entries = new HashMap<>();

    void addEntry(Name name, Log log) {
      errors.checkFatal(!entries.containsKey(name),"Log entry of name %s already exists", name);
      entries.put(name, log);
    }

    @Override
    public Optional<NamespaceObject> getNamespaceObject(Name name) {
      return Optional.ofNullable(entries.get(name)).map(log ->
          new TableSourceSinkNamespaceObject(log.getSource(), log.getSink(), tableFactory));
    }

    @Override
    public List<NamespaceObject> getNamespaceObjects() {
      return entries.values().stream().map(log ->
          new TableSourceSinkNamespaceObject(log.getSource(), log.getSink(), tableFactory))
          .collect(Collectors.toList());
    }
  }

}
