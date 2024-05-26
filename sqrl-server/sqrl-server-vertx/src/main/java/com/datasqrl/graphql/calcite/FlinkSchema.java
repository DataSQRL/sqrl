package com.datasqrl.graphql.calcite;

import com.google.common.base.Preconditions;
import java.sql.Connection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import lombok.SneakyThrows;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.table.jdbc.DriverUri;
import org.apache.flink.table.jdbc.FlinkConnection;
import org.apache.flink.table.jdbc.FlinkStatement;

public class FlinkSchema extends AbstractSchema {

  private final String url;
  private final int connectionTimeout;
  private final int idlenessTimeout;
  Map<String, Table> tableMap = new HashMap<>();
  Set<String> catalogQueries = new HashSet<>();
  static Connection connection;

  public FlinkSchema(String url, int connectionTimeout, int idlenessTimeout) {
    this.url = url;
    this.connectionTimeout = connectionTimeout;
    this.idlenessTimeout = idlenessTimeout;
  }

  public void assureOpen() {
    getConnection();
  }

  @Override
  protected Map<String, Table> getTableMap() {
    return tableMap;
  }

  public void addTable(FlinkQuery flinkQuery) {
    catalogQueries.addAll(flinkQuery.getCatalogQueries());
    Table table = tableMap.put(flinkQuery.getName(), new FlinkQueryTable(this, flinkQuery));
    Preconditions.checkState(table == null,
        "Duplicate table name found: %s", flinkQuery.getName());
  }

  @SneakyThrows
  protected Connection getConnection() {
    if (connection == null || connection.isClosed()) {
      connection = openConnection();
      createCatalog(connection);
    }
    return connection;
  }

  @SneakyThrows
  private Connection openConnection() {
    Properties properties = new Properties();
    properties.setProperty(RestOptions.CONNECTION_TIMEOUT.key(), Integer.toString(this.connectionTimeout));
    properties.setProperty(RestOptions.IDLENESS_TIMEOUT.key(), Integer.toString(this.idlenessTimeout));

    return new FlinkConnection(DriverUri.create(this.url, properties));
  }

  @SneakyThrows
  private void createCatalog(Connection connection) {
    FlinkStatement statement = (FlinkStatement) connection.createStatement();
    for (String query : catalogQueries) {
      statement.execute(query);
    }
  }
}
