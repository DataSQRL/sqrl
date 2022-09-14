package ai.datasqrl.physical.stream;

import ai.datasqrl.config.provider.JDBCConnectionProvider;
import ai.datasqrl.physical.PhysicalPlan;
import ai.datasqrl.physical.database.ddl.SqlDDLStatement;
import ai.datasqrl.physical.stream.flink.plan.FlinkStreamPhysicalPlan;
import lombok.AllArgsConstructor;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableResult;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.stream.Collectors;

@AllArgsConstructor
public class PhysicalPlanExecutor {

  public Job execute(PhysicalPlan physicalPlan) {
    executeDml(physicalPlan.getDatabaseDDL(), physicalPlan.getDbConnection());
    String executionId = executeFlink((FlinkStreamPhysicalPlan) physicalPlan.getStreamQueries());
    return new Job(executionId);
  }

  public void executeDml(List<SqlDDLStatement> dmlQueries, JDBCConnectionProvider dbConnection) {
    String dmls = dmlQueries.stream().map(ddl -> ddl.toSql()).collect(Collectors.joining("\n"));
    try (Connection conn = dbConnection.getConnection(); Statement stmt = conn.createStatement()) {
      System.out.println("Creating: "+ dmls);
      stmt.executeUpdate(dmls);
    } catch (SQLException e) {
      throw new RuntimeException("Could not execute SQL query", e);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Could not load database driver", e);
    }
  }

  public String executeFlink(FlinkStreamPhysicalPlan flinkPlan) {
    StatementSet statementSet = flinkPlan.getStatementSet();
    TableResult rslt = statementSet.execute();
    rslt.print(); //todo: this just forces print to wait for the async
    return rslt.getJobClient().get()
        .getJobID().toString();
  }
}
