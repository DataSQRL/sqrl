package ai.datasqrl.plan;

import ai.datasqrl.schema.Schema;
import java.util.List;
import lombok.Value;

@Value
public class LogicalPlan {
  List<RelQuery> streamQueries;
  List<RelQuery> databaseQueries;
  Schema schema;
}
