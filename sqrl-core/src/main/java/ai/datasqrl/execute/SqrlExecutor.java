package ai.datasqrl.execute;

import ai.datasqrl.config.provider.JDBCConnectionProvider;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;

public class SqrlExecutor {

  public void executeDml(JDBCConnectionProvider configuration, List<String> dmlQueries) {
    String dmls = dmlQueries.stream().collect(Collectors.joining("\n"));
    try (Connection conn = configuration.getConnection(); Statement stmt = conn.createStatement()) {
      stmt.executeUpdate(dmls);
    } catch (SQLException e) {
      throw new RuntimeException("Could not execute SQL query",e);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Could not load database driver",e);
    }
  }

  public void executeFlink(StreamStatementSet statementSet) {
    TableResult rslt = statementSet.execute();
    rslt.print();
  }
}
