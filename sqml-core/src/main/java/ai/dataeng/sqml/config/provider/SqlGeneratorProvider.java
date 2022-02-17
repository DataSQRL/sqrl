package ai.dataeng.sqml.config.provider;

import ai.dataeng.sqml.execution.sql.SQLGenerator;

public interface SqlGeneratorProvider {
  SQLGenerator create(JDBCConnectionProvider jdbcConnection);
}
