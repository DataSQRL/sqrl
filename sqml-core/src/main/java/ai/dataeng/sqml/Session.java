package ai.dataeng.sqml;

import java.sql.Connection;
import java.util.Optional;

public class Session {

  private final Connection connection;

  public Session(Connection connection) {
    this.connection = connection;
  }

  public Optional<String> getCatalog() {
    return Optional.of("catalog");
  }

  public Optional<String> getSchema() {
    return Optional.of("default");
  }

  public Connection getConnection() {
    return connection;
  }
}
