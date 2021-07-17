package ai.dataeng.sqml.migrate;

import ai.dataeng.sqml.PostgresResult;
import ai.dataeng.sqml.PostgresResult.MigrationObject;
import ai.dataeng.sqml.dag.Dag;
import com.google.common.base.Preconditions;
import java.sql.Connection;
import java.sql.SQLException;

public class PostgresSqmlMigration {
  private final Connection connection;
  private final Dag dag;

  public PostgresSqmlMigration(Connection connection, Dag dag) {
    this.connection = connection;
    this.dag = dag;
  }

  public void migrate() {
    Preconditions.checkState(dag.getOptimizationResult() instanceof PostgresResult, "Should be postgres optimizer result");
    PostgresResult result = (PostgresResult)dag.getOptimizationResult();
    for (MigrationObject migrationObject : result.getMigrationObjects()) {
      try {
        connection.createStatement().execute(migrationObject.getSql());
      } catch (SQLException throwables) {
        throw new RuntimeException("Could not migrate schema", throwables);
      }
    }

    System.out.println("Migration complete");
  }

  public static Builder newSqmlMigration() {
    return new Builder();
  }

  public static class Builder {

    private Connection connection;
    private Dag dag;

    public Builder dag(Dag dag) {
      this.dag = dag;
      return this;
    }
    public Builder session(Connection connection) {
      this.connection = connection;
      return this;
    }

    public PostgresSqmlMigration build() {
      return new PostgresSqmlMigration(connection, dag);
    }
  }
}
