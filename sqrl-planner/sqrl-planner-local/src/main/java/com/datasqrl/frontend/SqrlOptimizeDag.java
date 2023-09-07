package com.datasqrl.frontend;

import com.datasqrl.calcite.OperatorTable;
import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.flink.function.BridgingSqlScalarFunction;
import com.datasqrl.graphql.APIConnectorManager;
import com.datasqrl.graphql.server.Model.RootGraphqlModel;
import com.datasqrl.loaders.ModuleLoader;
import com.datasqrl.parse.SqrlParser;
import com.datasqrl.plan.global.DAGPlanner;
import com.datasqrl.plan.global.PhysicalDAGPlan;
import com.datasqrl.plan.local.generate.*;
import com.datasqrl.plan.table.CalciteTableFactory;
import com.google.inject.Inject;
import java.util.List;
import org.apache.calcite.jdbc.CalciteSchema;

import java.net.URL;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.calcite.jdbc.SqrlSchema;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.tools.RelBuilder;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.UserDefinedFunction;

public class SqrlOptimizeDag extends SqrlPlan {

  @Inject
  public SqrlOptimizeDag(SqrlParser parser,
      ErrorCollector errors,
      ModuleLoader moduleLoader,
      NameCanonicalizer nameCanonicalizer,
      CalciteTableFactory tableFactory,
      SqrlQueryPlanner planner,
      DebuggerConfig debuggerConfig,
      SqrlFramework framework,
      ExecutionPipeline pipeline) {
    super(parser, errors, moduleLoader, nameCanonicalizer, tableFactory, planner,
        debuggerConfig, framework, pipeline);
  }

  public PhysicalDAGPlan planDag(
      SqrlFramework framework,
      ExecutionPipeline pipeline,
      APIConnectorManager apiManager,
      RootGraphqlModel model,
      boolean includeJars) {
    DAGPlanner dagPlanner = new DAGPlanner(framework, pipeline, getDebugger(), errors);
    return dagPlanner.plan(framework.getSchema(), apiManager,
        framework.getSchema().getExports(),
        includeJars ? framework.getSchema().getJars() : Set.of(),
        extractFlinkFunctions(framework.getSqrlOperatorTable()), model);
  }

  private Map<String, UserDefinedFunction> extractFlinkFunctions(OperatorTable sqrlOperatorTable) {
    Map<String, UserDefinedFunction> fncs = new HashMap<>();
    for (Map.Entry<List<String>, SqlFunction> fnc : sqrlOperatorTable.getUdfs().entrySet()) {
      if (fnc.getValue() instanceof BridgingSqlScalarFunction) {
        FunctionDefinition definition = ((BridgingSqlScalarFunction) fnc.getValue()).getDefinition();
        if (definition instanceof UserDefinedFunction) {

          fncs.put(fnc.getKey().get(0),
              (UserDefinedFunction) definition);
        }

      }
    }
    return fncs;
  }
}
