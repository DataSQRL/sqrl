package com.datasqrl.frontend;

import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.loaders.ModuleLoader;
import com.datasqrl.loaders.ModuleLoaderComposite;
import com.datasqrl.parse.SqrlParser;
import com.datasqrl.plan.local.generate.*;
import com.datasqrl.plan.table.CalciteTableFactory;
import com.google.inject.Inject;
import org.apache.calcite.sql.ScriptNode;

import java.util.List;

public class SqrlPlan extends SqrlParse {

  protected NamespaceFactory nsFactory;
  protected ModuleLoader moduleLoader;
  protected NameCanonicalizer nameCanonicalizer;
  protected CalciteTableFactory tableFactory;
  protected SqrlQueryPlanner planner;
  private final DebuggerConfig debuggerConfig;

  @Inject
  public SqrlPlan(SqrlParser parser, ErrorCollector errors, NamespaceFactory nsFactory,
      ModuleLoader moduleLoader, NameCanonicalizer nameCanonicalizer, CalciteTableFactory tableFactory,
      SqrlQueryPlanner planner, DebuggerConfig debuggerConfig) {
    super(parser, errors);
    this.nsFactory = nsFactory;
    this.moduleLoader = moduleLoader;
    this.nameCanonicalizer = nameCanonicalizer;
    this.tableFactory = tableFactory;
    this.planner = planner;
    this.debuggerConfig = debuggerConfig;
  }

  public Namespace plan(String script, List<ModuleLoader> additionalModules) {
    return plan(parse(script), additionalModules);
  }

  public Namespace plan(ScriptNode node, List<ModuleLoader> additionalModules) {
    ModuleLoader updatedModuleLoader = this.moduleLoader;
    if (!additionalModules.isEmpty()) {
      updatedModuleLoader = ModuleLoaderComposite.builder()
              .moduleLoader(this.moduleLoader)
              .moduleLoaders(additionalModules)
              .build();
    }
    Resolve resolve = new Resolve(this.nsFactory, updatedModuleLoader,
        this.nameCanonicalizer, this.errors,
        new StatementProcessor(updatedModuleLoader, nameCanonicalizer,planner, tableFactory));

    Namespace namespace = resolve.planTables(node);

    return namespace;
  }

  public Debugger getDebugger() {
    return new Debugger(debuggerConfig, moduleLoader);
  }

}
