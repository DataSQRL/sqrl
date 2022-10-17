package ai.datasqrl.util.db;

import ai.datasqrl.config.EnvironmentConfiguration.MetaData;
import ai.datasqrl.config.engines.JDBCConfiguration;
import ai.datasqrl.config.engines.JDBCConfiguration.Dialect;
import java.util.Properties;
import lombok.Getter;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

public class JDBCTempDatabase {

  private final Properties properties;
  @Getter
  private final PostgreSQLContainer postgreSQLContainer;

  public JDBCTempDatabase() {
    DockerImageName image = DockerImageName.parse("postgres:14.2");
    postgreSQLContainer = new PostgreSQLContainer(image)
        .withDatabaseName(MetaData.DEFAULT_DATABASE);
    postgreSQLContainer.start();

    this.properties = new Properties();
    this.properties.put("db.dialect", Dialect.POSTGRES.toString());
    this.properties.put("db.driverClassName", "org.postgresql.Driver");
    this.properties.put("db.username", "test");
    this.properties.put("db.password", "test");
    this.properties.put("db.url", postgreSQLContainer.getJdbcUrl().substring(0,
        postgreSQLContainer.getJdbcUrl().lastIndexOf("/")));
  }

  public JDBCConfiguration getJdbcConfiguration() {
    return JDBCConfiguration.builder()
        .dbURL((String)properties.get("db.url"))
        .driverName((String)properties.get("db.driverClassName"))
        .dialect(Dialect.valueOf((String)properties.get("db.dialect")))
        .user((String)properties.get("db.username"))
        .password((String)properties.get("db.password"))
        .build();
  }

  public void cleanUp() {
    if (postgreSQLContainer!=null) {
      postgreSQLContainer.stop();
    }
  }
}
