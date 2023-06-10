package com.datasqrl.frontend;

import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.engine.PhysicalPlan;
import com.datasqrl.engine.PhysicalPlanner;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.loaders.ModuleLoader;
import com.datasqrl.parse.SqrlParser;
import com.datasqrl.plan.global.PhysicalDAGPlan;
import com.datasqrl.plan.local.generate.DebuggerConfig;
import com.datasqrl.plan.local.generate.NamespaceFactory;
import com.datasqrl.plan.local.generate.SqrlQueryPlanner;
import com.datasqrl.plan.table.CalciteTableFactory;
import com.google.inject.Inject;
import lombok.Getter;

@Getter
public class SqrlPhysicalPlan extends SqrlOptimizeDag {
  protected PhysicalPlanner physicalPlanner;

  @Inject
  public SqrlPhysicalPlan(SqrlParser parser,
      ErrorCollector errors,
      NamespaceFactory nsFactory,
      ModuleLoader moduleLoader,
      NameCanonicalizer nameCanonicalizer,
      CalciteTableFactory tableFactory,
      SqrlQueryPlanner planner, DebuggerConfig debuggerConfig,
      ErrorSink errorSink) {
    super(parser, errors, nsFactory, moduleLoader, nameCanonicalizer, tableFactory, planner, debuggerConfig);
    physicalPlanner = new PhysicalPlanner(planner.createRelBuilder(), errorSink.getErrorSink());
  }

  public PhysicalPlan createPhysicalPlan(PhysicalDAGPlan dag) {
    PhysicalPlan physicalPlan = physicalPlanner.plan(dag);
    return physicalPlan;
  }
}
