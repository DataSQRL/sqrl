package com.datasqrl.frontend;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.engine.PhysicalPlan;
import com.datasqrl.engine.PhysicalPlanner;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.loaders.ModuleLoader;
import com.datasqrl.parse.SqrlParser;
import com.datasqrl.plan.global.PhysicalDAGPlan;
import com.datasqrl.plan.local.generate.DebuggerConfig;
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
      ModuleLoader moduleLoader,
      NameCanonicalizer nameCanonicalizer,
      CalciteTableFactory tableFactory,
      SqrlQueryPlanner planner, DebuggerConfig debuggerConfig,
      ErrorSink errorSink,
      SqrlFramework framework,
      ExecutionPipeline pipeline) {
    super(parser, errors, moduleLoader, nameCanonicalizer, tableFactory, planner,
        debuggerConfig, framework, pipeline);
    physicalPlanner = new PhysicalPlanner(framework, errorSink.getErrorSink());
  }

  public PhysicalPlan createPhysicalPlan(PhysicalDAGPlan dag) {
    PhysicalPlan physicalPlan = physicalPlanner.plan(dag);
    return physicalPlan;
  }
}
