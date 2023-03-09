package com.datasqrl.frontend;

import com.datasqrl.engine.PhysicalPlan;
import com.datasqrl.engine.PhysicalPlanner;
import com.datasqrl.error.ErrorCode;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.tables.TableSink;
import com.datasqrl.loaders.DataSystemNsObject;
import com.datasqrl.loaders.ModuleLoader;
import com.datasqrl.name.NameCanonicalizer;
import com.datasqrl.name.NamePath;
import com.datasqrl.parse.SqrlParser;
import com.datasqrl.plan.global.OptimizedDAG;
import com.datasqrl.plan.local.generate.DebuggerConfig;
import com.datasqrl.plan.local.generate.NamespaceFactory;
import com.datasqrl.plan.local.generate.SqrlQueryPlanner;
import com.datasqrl.plan.local.generate.StatementProcessor;
import com.google.inject.Inject;
import java.util.Optional;
import javax.validation.constraints.NotEmpty;
import lombok.Getter;
import lombok.NonNull;

@Getter
public class SqrlPhysicalPlan extends SqrlOptimizeDag {
  protected PhysicalPlanner physicalPlanner;

  @Inject
  public SqrlPhysicalPlan(SqrlParser parser,
      ErrorCollector errors,
      NamespaceFactory nsFactory,
      ModuleLoader moduleLoader,
      NameCanonicalizer nameCanonicalizer,
      StatementProcessor statementProcessor,
      SqrlQueryPlanner planner, DebuggerConfig debuggerConfig,
      ErrorSink errorSink) {
    super(parser, errors, nsFactory, moduleLoader, nameCanonicalizer, statementProcessor, planner, debuggerConfig);
    physicalPlanner = new PhysicalPlanner(planner.createRelBuilder(), errorSink.getErrorSink());
  }

  public PhysicalPlan createPhysicalPlan(OptimizedDAG dag) {
    PhysicalPlan physicalPlan = physicalPlanner.plan(dag);
    return physicalPlan;
  }
}
