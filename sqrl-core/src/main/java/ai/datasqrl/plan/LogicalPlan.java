package ai.datasqrl.plan;

import ai.datasqrl.plan.queries.TableQuery;
import ai.datasqrl.schema.Schema;
import java.util.List;
import lombok.Value;

@Value
public class LogicalPlan {
  List<TableQuery> streamQueries;
  List<TableQuery> databaseQueries;
  Schema schema;
}
