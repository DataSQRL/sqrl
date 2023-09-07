package com.datasqrl.frontend;

import com.datasqrl.SqrlPlanningTableFactory;
import com.datasqrl.calcite.Dialect;
import com.datasqrl.calcite.QueryPlanner;
import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.calcite.schema.ScriptExecutor;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.loaders.ModuleLoader;
import com.datasqrl.loaders.ModuleLoaderComposite;
import com.datasqrl.parse.SqrlParser;
import com.datasqrl.plan.local.generate.*;
import com.datasqrl.plan.table.CalciteTableFactory;
import com.datasqrl.util.SqlNameUtil;
import com.google.inject.Inject;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.ScriptNode;
import org.apache.calcite.sql.SqlNode;

import java.util.List;

public class SqrlPlan extends SqrlParse {

  private final DebuggerConfig debuggerConfig;
  private final SqrlFramework framework;
  private final ExecutionPipeline pipeline;
  protected ModuleLoader moduleLoader;
  protected NameCanonicalizer nameCanonicalizer;
  protected CalciteTableFactory tableFactory;
  protected SqrlQueryPlanner planner;

  @Inject
  public SqrlPlan(SqrlParser parser, ErrorCollector errors,
      ModuleLoader moduleLoader, NameCanonicalizer nameCanonicalizer,
      CalciteTableFactory tableFactory,
      SqrlQueryPlanner planner, DebuggerConfig debuggerConfig, SqrlFramework framework,
      ExecutionPipeline pipeline) {
    super(parser, errors);
    this.moduleLoader = moduleLoader;
    this.nameCanonicalizer = nameCanonicalizer;
    this.tableFactory = tableFactory;
    this.planner = planner;
    this.debuggerConfig = debuggerConfig;
    this.framework = framework;
    this.pipeline = pipeline;
  }

  public Namespace plan(String script, List<ModuleLoader> additionalModules) {
    ScriptNode scriptNode = (ScriptNode) framework.getQueryPlanner().parse(Dialect.SQRL,
        script);

    return plan(scriptNode, additionalModules);
  }

  public Namespace plan(ScriptNode node, List<ModuleLoader> additionalModules) {
    ModuleLoader updatedModuleLoader = this.moduleLoader;
    if (!additionalModules.isEmpty()) {
      updatedModuleLoader = ModuleLoaderComposite.builder()
          .moduleLoader(this.moduleLoader)
          .moduleLoaders(additionalModules)
          .build();
    }

    QueryPlanner qPlanner = framework.getQueryPlanner();
    ScriptExecutor executor = new ScriptExecutor(new SqrlPlanningTableFactory(framework, nameCanonicalizer), framework,
        new SqlNameUtil(nameCanonicalizer), updatedModuleLoader, errors);
    for (SqlNode statement : node.getStatements()) {
      RelNode relNode = qPlanner.plan(Dialect.SQRL, statement);
      executor.apply(relNode);
    }

    return new Namespace(framework, pipeline);
  }

  public Debugger getDebugger() {
    return new Debugger(debuggerConfig, moduleLoader);
  }

}
