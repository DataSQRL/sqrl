package com.datasqrl.plan;

import com.datasqrl.calcite.OperatorTable;
import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.graphql.APIConnectorManager;
import com.datasqrl.graphql.server.Model.RootGraphqlModel;
import com.datasqrl.plan.global.DAGPlanner;
import com.datasqrl.plan.global.PhysicalDAGPlan;
import com.datasqrl.plan.local.generate.Debugger;
import com.datasqrl.util.FunctionUtil;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.calcite.sql.SqlOperator;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.UserDefinedFunction;

public class SqrlOptimizeDag {

  public static PhysicalDAGPlan planDag(
      SqrlFramework framework,
      ExecutionPipeline pipeline,
      APIConnectorManager apiManager,
      RootGraphqlModel model,
      boolean includeJars,
      Debugger debugger,
      ErrorCollector errors) {
    return DAGPlanner.planPhysical(framework, apiManager,
        framework.getSchema().getExports(),
        includeJars ? framework.getSchema().getJars() : Set.of(),
        extractFlinkFunctions(framework.getSqrlOperatorTable()), model,pipeline,
        errors, debugger);
  }

  public static Map<String, UserDefinedFunction> extractFlinkFunctions(OperatorTable sqrlOperatorTable) {
    Map<String, UserDefinedFunction> fncs = new HashMap<>();
    for (Map.Entry<String, SqlOperator> fnc : sqrlOperatorTable.getUdfs().entrySet()) {
      Optional<FunctionDefinition> definition = FunctionUtil.getSqrlFunction(fnc.getValue());
      if (definition.isPresent()) {
        if (definition.get() instanceof UserDefinedFunction) {
          fncs.put(fnc.getKey(), (UserDefinedFunction)definition.get());
        }
      }
    }
    return fncs;
  }
}
