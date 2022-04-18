package ai.datasqrl.execute;

import ai.datasqrl.config.provider.JDBCConnectionProvider;
import ai.datasqrl.physical.ExecutionPlan;
import ai.datasqrl.physical.database.ddl.SqlDDLStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;

public class ScriptExecutor {
  JDBCConnectionProvider configuration;

  public Job execute(ExecutionPlan executionPlan) {
    String executionId = executeFlink(executionPlan.getStreamQueries());
    return new Job(executionId);
  }

  public void executeDml(List<SqlDDLStatement> dmlQueries) {
//    String dmls = dmlQueries.stream().collect(Collectors.joining("\n"));
    try (Connection conn = configuration.getConnection(); Statement stmt = conn.createStatement()) {
//      stmt.executeUpdate(dmls);
    } catch (SQLException e) {
      throw new RuntimeException("Could not execute SQL query",e);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Could not load database driver",e);
    }
  }

  public String executeFlink(StreamStatementSet statementSet) {
    TableResult rslt = statementSet.execute();
    return rslt.getJobClient().get()
        .getJobID().toString();
  }
}
