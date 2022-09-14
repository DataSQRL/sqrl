package ai.datasqrl.physical.database;

import ai.datasqrl.plan.calcite.util.RelToSql;
import ai.datasqrl.plan.global.OptimizedDAG;
import ai.datasqrl.plan.queries.APIQuery;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class QueryBuilder {

  public Map<APIQuery, QueryTemplate> planQueries(List<OptimizedDAG.ReadQuery> databaseQueries) {
    Map<APIQuery, QueryTemplate> resultQueries = new HashMap<>();
    for (OptimizedDAG.ReadQuery query : databaseQueries) {
      resultQueries.put(query.getQuery(), planQuery(query));
    }
    return resultQueries;
  }

  private QueryTemplate planQuery(OptimizedDAG.ReadQuery query) {
    return new QueryTemplate(RelToSql.convertToSql(query.getRelNode()));
  }

}
