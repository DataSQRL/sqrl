package ai.datasqrl.plan;

import ai.datasqrl.plan.queries.TableQuery;
import java.util.List;
import lombok.Value;
import org.apache.calcite.schema.Schema;

@Value
public class LogicalPlan {

  List<TableQuery> streamQueries;
  List<TableQuery> databaseQueries;
  Schema schema;
}
