package ai.dataeng.sqml.parser.sqrl;

import ai.dataeng.sqml.config.provider.JDBCConnectionProvider;
import ai.dataeng.sqml.execution.sql.DatabaseSink;
import ai.dataeng.sqml.execution.sql.util.CreateTableBuilder;
import ai.dataeng.sqml.execution.sql.util.DatabaseUtil;
import ai.dataeng.sqml.parser.Table;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Value;
import org.apache.flink.table.api.TableDescriptor;

public class SqlGenerator {

  private final Map<Table, TableDescriptor> tables;

  public SqlGenerator(Map<Table, TableDescriptor> tables) {

    this.tables = tables;
  }

  public List<String> generate() {
    //1. Generate DML strings
    List<String> dml = new ArrayList<>();

    for (Map.Entry<Table, TableDescriptor> entry : tables.entrySet()) {

      CreateTableBuilder tableBuilder = new CreateTableBuilder(entry.getValue().getOptions().get("table-name"))
          .addColumns(entry.getValue().getSchema().get(), entry.getKey());
      dml.add(tableBuilder.getSQL());
    }

    return dml;
  }

//
//    @Value
//    public static class Result {
//        List<String> dmlQueries;
//
//        public void executeDMLs(JDBCConnectionProvider configuration) {
//            String dmls = dmlQueries.stream().collect(Collectors.joining("\n"));
//            System.out.println(dmls);
//            try (Connection conn = configuration.getConnection(); Statement stmt = conn.createStatement()) {
//                stmt.executeUpdate(dmls);
//            } catch (SQLException e) {
//                throw new RuntimeException("Could not execute SQL query",e);
//            } catch (ClassNotFoundException e) {
//                throw new RuntimeException("Could not load database driver",e);
//            }
//        }
//    }
}
