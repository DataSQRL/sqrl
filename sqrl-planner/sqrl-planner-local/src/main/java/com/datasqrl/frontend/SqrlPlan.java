package com.datasqrl.frontend;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.loaders.ModuleLoader;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.loaders.ModuleLoaderComposite;
import com.datasqrl.parse.SqrlParser;
import com.datasqrl.plan.local.generate.Debugger;
import com.datasqrl.plan.local.generate.DebuggerConfig;
import com.datasqrl.plan.local.generate.Namespace;
import com.datasqrl.plan.local.generate.NamespaceFactory;
import com.datasqrl.plan.local.generate.Resolve;
import com.datasqrl.plan.local.generate.SqrlQueryPlanner;
import com.datasqrl.plan.local.generate.StatementProcessor;
import com.datasqrl.plan.queries.APIConnectors;
import com.google.inject.Inject;
import java.util.List;
import org.apache.calcite.sql.ScriptNode;

public class SqrlPlan extends SqrlParse {

  protected NamespaceFactory nsFactory;
  protected ModuleLoader moduleLoader;
  protected NameCanonicalizer nameCanonicalizer;
  protected StatementProcessor statementProcessor;
  protected SqrlQueryPlanner planner;
  private final DebuggerConfig debuggerConfig;

  @Inject
  public SqrlPlan(SqrlParser parser, ErrorCollector errors, NamespaceFactory nsFactory,
      ModuleLoader moduleLoader, NameCanonicalizer nameCanonicalizer,
      StatementProcessor statementProcessor, SqrlQueryPlanner planner, DebuggerConfig debuggerConfig) {
    super(parser, errors);
    this.nsFactory = nsFactory;
    this.moduleLoader = moduleLoader;
    this.nameCanonicalizer = nameCanonicalizer;
    this.statementProcessor = statementProcessor;
    this.planner = planner;
    this.debuggerConfig = debuggerConfig;
  }

  public Namespace plan(String script, List<ModuleLoader> additionalModules) {
    return plan(parse(script), additionalModules);
  }

  public Namespace plan(ScriptNode node, List<ModuleLoader> additionalModules) {
    ModuleLoader updatedModuleLoader = this.moduleLoader;
    if (!additionalModules.isEmpty()) {
      updatedModuleLoader = ModuleLoaderComposite.builder().
          moduleLoader(this.moduleLoader).
          moduleLoaders(additionalModules).
          build();
    }
    Resolve resolve = new Resolve(this.nsFactory, updatedModuleLoader,
        this.nameCanonicalizer, this.errors, this.statementProcessor);

    Namespace namespace = resolve.planTables(node);

    return namespace;
  }

  public Debugger getDebugger() {
    return new Debugger(debuggerConfig, moduleLoader);
  }

}
