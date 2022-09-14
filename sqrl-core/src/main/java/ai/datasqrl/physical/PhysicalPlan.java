package ai.datasqrl.physical;

import ai.datasqrl.config.provider.JDBCConnectionProvider;
import ai.datasqrl.physical.database.QueryTemplate;
import ai.datasqrl.physical.database.ddl.SqlDDLStatement;
import ai.datasqrl.physical.stream.StreamPhysicalPlan;
import ai.datasqrl.plan.queries.APIQuery;
import lombok.Value;

import java.util.List;
import java.util.Map;

@Value
public class PhysicalPlan {

  JDBCConnectionProvider dbConnection;
  List<SqlDDLStatement> databaseDDL;
  StreamPhysicalPlan streamQueries;
  Map<APIQuery, QueryTemplate> databaseQueries;

}
