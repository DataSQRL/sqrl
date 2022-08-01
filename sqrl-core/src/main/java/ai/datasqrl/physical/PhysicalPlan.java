package ai.datasqrl.physical;

import ai.datasqrl.config.provider.JDBCConnectionProvider;
import ai.datasqrl.physical.database.ddl.SqlDDLStatement;
import java.util.List;
import lombok.Value;
import org.apache.calcite.schema.Schema;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;

@Value
public class PhysicalPlan {

  JDBCConnectionProvider dbConnection;
  List<SqlDDLStatement> databaseDDL;
  StreamStatementSet streamQueries;
}
