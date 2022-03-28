package ai.dataeng.sqml.planner;

public interface ThePlan {
  /*
   * 1. Import source: imports RelNode, adds to logical plan.
   * 2. Add query: Parse -> Unsqrl -> add to logical plan
   * 3. Add dev mode queries: Add Sink nodes to all non-shadowed tables
   * 4. Optimize: Find cut point -> List of Flink 'Tables', List of Postgres Views
   * 5. Generate ddl, write to postgres, create flink plan
   * 6. Execute flink pipeline
   */

  /*
   * To Code:
   * 1. Import becomes a RelNode into logical plan
   * 2. Do a query, add to Logical Plan (all rel nodes w/ table labels)
   * 3. Add dev mode queries -> LogicalSink to all table labels
   * 4. Optimize LogicalPlan -> Take all sources, split plan
   * 5. Physical plan -> Generate sql, flink plan
   * 6. Wire up graphql
   */
}
