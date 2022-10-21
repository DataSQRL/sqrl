package ai.datasqrl.physical.stream;

import ai.datasqrl.config.provider.JDBCConnectionProvider;
import ai.datasqrl.physical.PhysicalPlan;
import ai.datasqrl.physical.database.relational.ddl.SqlDDLStatement;
import ai.datasqrl.physical.stream.flink.plan.FlinkStreamPhysicalPlan;
import ai.datasqrl.util.db.JDBCTempDatabase;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableResult;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.stream.Collectors;

@AllArgsConstructor
@Slf4j
public class PhysicalPlanExecutor {

  public Job execute(PhysicalPlan physicalPlan) {
    return execute(physicalPlan, Optional.empty());
  }

  public Job execute(PhysicalPlan physicalPlan, Optional<JDBCTempDatabase> jdbcTempDatabase) {
    executeDml(physicalPlan.getDatabaseDDL(), physicalPlan.getDbConnection(), jdbcTempDatabase);
    String executionId = executeFlink((FlinkStreamPhysicalPlan) physicalPlan.getStreamQueries());
    return new Job(executionId);
  }

  @SneakyThrows
  public void executeDml(List<SqlDDLStatement> dmlQueries, JDBCConnectionProvider dbConnection,
      Optional<JDBCTempDatabase> jdbcTempDatabase) {
    String dmls = dmlQueries.stream().map(ddl -> ddl.toSql()).collect(Collectors.joining("\n"));

    Connection conn;
    if (jdbcTempDatabase.isPresent()) {
      conn = jdbcTempDatabase.get().getPostgreSQLContainer().createConnection("");
    } else {
      conn = dbConnection.getConnection();
    }


    try (Statement stmt = conn.createStatement()) {
      log.info("Creating: "+ dmls);
      stmt.executeUpdate(dmls);
    } catch (SQLException e) {
      throw new RuntimeException("Could not execute SQL query", e);
//    } catch (ClassNotFoundException e) {
//      throw new RuntimeException("Could not load database driver", e);
    } finally {
      conn.close();
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
