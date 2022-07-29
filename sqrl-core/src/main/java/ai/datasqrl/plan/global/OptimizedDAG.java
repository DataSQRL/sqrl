package ai.datasqrl.plan.global;

import ai.datasqrl.plan.queries.TableQuery;
import lombok.Value;
import org.apache.calcite.schema.Schema;

import java.util.List;

@Value
public class OptimizedDAG {

  List<TableQuery> streamQueries;
  List<TableQuery> databaseQueries;
  Schema schema;





}
