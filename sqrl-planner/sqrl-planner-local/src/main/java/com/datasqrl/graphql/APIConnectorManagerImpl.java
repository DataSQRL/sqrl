package com.datasqrl.graphql;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.engine.ExecutionEngine.Type;
import com.datasqrl.engine.log.Log;
import com.datasqrl.engine.log.LogEngine;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.graphql.inference.SqrlSchema2.SQRLTable;
import com.datasqrl.io.tables.TableSink;
import com.datasqrl.io.tables.TableSource;
import com.datasqrl.loaders.ModuleLoader;
import com.datasqrl.loaders.ModuleLoaderPreloaded;
import com.datasqrl.loaders.TableSourceSinkNamespaceObject;
import com.datasqrl.module.NamespaceObject;
import com.datasqrl.module.SqrlModule;
import com.datasqrl.plan.queries.APIMutation;
import com.datasqrl.plan.queries.APIQuery;
import com.datasqrl.plan.queries.APISource;
import com.datasqrl.plan.queries.APISubscription;
import com.datasqrl.plan.table.CalciteTableFactory;
import com.datasqrl.plan.table.RelDataType2UTBConverter;
import com.datasqrl.plan.table.ScriptRelationalTable;
import com.datasqrl.schema.UniversalTable;
import com.google.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.Getter;
import org.apache.calcite.rel.type.RelDataTypeFactory;

@Getter
public class APIConnectorManagerImpl implements APIConnectorManager {

  private final CalciteTableFactory tableFactory;
  private final Optional<LogEngine> logEngine;
  private final ErrorCollector errors;
  private final ModuleLoader moduleLoader;
  private final RelDataTypeFactory typeFactory;

  private final Map<APIMutation, TableSink> mutations = new HashMap<>();

  private final Map<NamePath, LogModule> modules = new HashMap<>();

  private final Map<APISubscription, TableSource> subscriptions = new HashMap<>();

  private final Map<SQRLTable, Log> exports = new HashMap<>();

  private final List<APIQuery> queries = new ArrayList<>();


  @Inject
  public APIConnectorManagerImpl(CalciteTableFactory tableFactory, ExecutionPipeline pipeline,
      ErrorCollector errors, ModuleLoader moduleLoader, RelDataTypeFactory typeFactory) {
    this.tableFactory = tableFactory;
    this.errors = errors;
    this.moduleLoader = moduleLoader;
    this.logEngine = pipeline.getStage(Type.LOG).map(stage -> (LogEngine) stage.getEngine());
    this.typeFactory = typeFactory;
  }

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
      mutations.put(mutation, sourceSink.getSink());
    } else {
      //Create module if log engine is set
      errors.checkFatal(logEngine.isPresent(), "Cannot create mutation %s: Could not load "
          + "module for %s and no log engine configured", mutation, apiNamePath);
      LogModule logModule = modules.get(apiNamePath);
      if (logModule==null) {
        logModule = new LogModule();
        modules.put(apiNamePath, logModule);
      }
      String logId = getLogId(mutation);
      Log log = logEngine.get().createLog(logId, mutation.getSchema());
      logModule.addEntry(mutation.getName(),log);
      mutations.put(mutation, log.getSink());
    }
  }

  @Override
  public TableSink getMutationSource(APISource source, Name mutationName) {
    return mutations.get(new APIMutation(mutationName, source, null));
  }

  @Override
  public TableSource addSubscription(APISubscription subscription, SQRLTable sqrlTable) {
    errors.checkFatal(logEngine.isPresent(), "Cannot create subscriptions because no log engine is configured");
//    errors.checkFatal(((ScriptRelationalTable) sqrlTable.getVt()).getRoot().getType()== TableType.STREAM,
//        "Table %s for subscription %s is not a stream table", sqrlTable, subscription);
    //Check if we already exported it
    TableSource subscriptionSource = null;
    if (exports.containsKey(sqrlTable)) {
      subscriptionSource = exports.get(sqrlTable).getSource();
    } else {
      //otherwise create new log for it
      String logId = ((ScriptRelationalTable) sqrlTable.getVt()).getNameId();
      RelDataType2UTBConverter converter = new RelDataType2UTBConverter(typeFactory, 0,
          NameCanonicalizer.SYSTEM);
      UniversalTable schema = converter.convert(
          NamePath.of(sqrlTable.getPath().toArray(String[]::new)),
          ((ScriptRelationalTable) sqrlTable.getVt()).getRowType(),
          null);
      Log log = logEngine.get().createLog(logId, schema);
      exports.put(sqrlTable, log);
      subscriptionSource = log.getSource();
    }
    subscriptions.put(subscription, subscriptionSource);
    return subscriptionSource;
  }

  @Override
  public void addQuery(APIQuery query) {
    queries.add(query);
  }

  @Override
  public ModuleLoader getAsModuleLoader() {
    return ModuleLoaderPreloaded.builder().modules(modules).build();
  }

  @Override
  public List<Log> getLogs() {
    List<Log> logs = new ArrayList<>();
    logs.addAll(exports.values());
    modules.values().stream()
        .flatMap(logModule -> logModule.entries.values().stream())
        .forEach(logs::add);
    return logs;
  }

  private NamePath apiToModulePath(APISource source) {
    return source.getName().toNamePath();
  }

  private String getLogId(APIMutation mutation) {
    return mutation.getSource().getName().getCanonical() + "-" + mutation.getName().getCanonical();
  }

  private class LogModule implements SqrlModule {

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
