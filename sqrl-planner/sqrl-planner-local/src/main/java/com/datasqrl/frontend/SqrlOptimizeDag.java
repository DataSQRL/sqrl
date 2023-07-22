package com.datasqrl.frontend;

import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.graphql.APIConnectorManager;
import com.datasqrl.graphql.server.Model.RootGraphqlModel;
import com.datasqrl.loaders.ModuleLoader;
import com.datasqrl.parse.SqrlParser;
import com.datasqrl.plan.global.DAGPlanner;
import com.datasqrl.plan.global.PhysicalDAGPlan;
import com.datasqrl.plan.local.generate.DebuggerConfig;
import com.datasqrl.plan.local.generate.Namespace;
import com.datasqrl.plan.local.generate.NamespaceFactory;
import com.datasqrl.plan.local.generate.SqrlQueryPlanner;
import com.datasqrl.plan.table.CalciteTableFactory;
import com.google.inject.Inject;
import org.apache.calcite.jdbc.CalciteSchema;

import java.util.Set;

public class SqrlOptimizeDag extends SqrlPlan {

  @Inject
  public SqrlOptimizeDag(SqrlParser parser,
      ErrorCollector errors,
      NamespaceFactory nsFactory,
      ModuleLoader moduleLoader,
      NameCanonicalizer nameCanonicalizer,
      CalciteTableFactory tableFactory,
      SqrlQueryPlanner planner,
      DebuggerConfig debuggerConfig) {
    super(parser, errors, nsFactory, moduleLoader, nameCanonicalizer, tableFactory, planner, debuggerConfig);
  }

  public PhysicalDAGPlan planDag(Namespace ns, APIConnectorManager apiManager, RootGraphqlModel model,
      boolean includeJars) {
    DAGPlanner dagPlanner = new DAGPlanner(planner.createRelBuilder(), planner, ns.getSchema().getPlanner(),
        ns.getSchema().getPipeline(), getDebugger(), errors);
    CalciteSchema relSchema = planner.getSchema();
    return dagPlanner.plan(relSchema, apiManager, ns.getExports(), includeJars ? ns.getJars() : Set.of(),
        ns.getUdfs(), model);
  }
}
