package com.datasqrl.frontend;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.loaders.ModuleLoader;
import com.datasqrl.name.NameCanonicalizer;
import com.datasqrl.parse.SqrlParser;
import com.datasqrl.plan.global.DAGPlanner;
import com.datasqrl.plan.global.PhysicalDAGPlan;
import com.datasqrl.plan.local.generate.DebuggerConfig;
import com.datasqrl.plan.local.generate.Namespace;
import com.datasqrl.plan.local.generate.NamespaceFactory;
import com.datasqrl.plan.local.generate.SqrlQueryPlanner;
import com.datasqrl.plan.local.generate.StatementProcessor;
import com.datasqrl.plan.queries.APIQuery;
import com.google.inject.Inject;
import java.util.Collection;
import java.util.Set;
import org.apache.calcite.jdbc.CalciteSchema;

public class SqrlOptimizeDag extends SqrlPlan {

  @Inject
  public SqrlOptimizeDag(SqrlParser parser,
      ErrorCollector errors,
      NamespaceFactory nsFactory,
      ModuleLoader moduleLoader,
      NameCanonicalizer nameCanonicalizer,
      StatementProcessor statementProcessor,
      SqrlQueryPlanner planner,
      DebuggerConfig debuggerConfig) {
    super(parser, errors, nsFactory, moduleLoader, nameCanonicalizer, statementProcessor, planner, debuggerConfig);
  }

  public PhysicalDAGPlan planDag(Namespace ns, Collection<APIQuery> queries) {
    return planDag(ns, queries, true);
  }

  public PhysicalDAGPlan planDag(Namespace ns, Collection<APIQuery> queries, boolean includeJars) {
    DAGPlanner dagPlanner = new DAGPlanner(planner.createRelBuilder(), ns.getSchema().getPlanner(),
        ns.getSchema().getPipeline(), getDebugger(), errors);
    CalciteSchema relSchema = planner.getSchema();
    return dagPlanner.plan(relSchema, queries, ns.getExports(), includeJars ? ns.getJars() : Set.of(),
        ns.getUdfs());
  }
}
