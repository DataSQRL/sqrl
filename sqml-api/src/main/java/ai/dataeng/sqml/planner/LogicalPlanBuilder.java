package ai.dataeng.sqml.planner;

import ai.dataeng.sqml.analyzer.StatementAnalysis;
import ai.dataeng.sqml.logical4.LogicalPlan;
import ai.dataeng.sqml.metadata.Metadata;
import ai.dataeng.sqml.tree.Query;
import ai.dataeng.sqml.tree.Statement;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class LogicalPlanBuilder {
  RowNodeIdAllocator idAllocator;
  Metadata metadata;
  LogicalPlan logicalPlan;

  private final PlanVariableAllocator variableAllocator = new PlanVariableAllocator();
  
  public RelationPlan planStatement(StatementAnalysis analysis, Statement statement)
  {
    return createRelationPlan(analysis, (Query) statement);
  }

  private RelationPlan createRelationPlan(StatementAnalysis analysis, Query query)
  {
    return new RelationPlanner(analysis, variableAllocator, idAllocator, metadata, logicalPlan)
        .process(query, null);
  }
}
