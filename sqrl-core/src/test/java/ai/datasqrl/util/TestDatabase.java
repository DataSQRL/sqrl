package ai.datasqrl.util;

import ai.datasqrl.config.EnvironmentConfiguration.MetaData;
import ai.datasqrl.config.engines.JDBCConfiguration;
import ai.datasqrl.config.engines.JDBCConfiguration.Dialect;
import ai.datasqrl.util.PropertiesUtil;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Properties;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.containers.wait.strategy.WaitStrategy;
import org.testcontainers.utility.DockerImageName;

public class TestDatabase {

  private final Properties properties;
  private final PostgreSQLContainer postgreSQLContainer;

  public TestDatabase() {
    if (hasLocalDbConfig()) {
      this.properties = PropertiesUtil.properties;
      this.postgreSQLContainer = null;
    } else {
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
  }

  public void stop() {
    if (postgreSQLContainer!=null) {
      postgreSQLContainer.stop();
    }
  }

  private static boolean hasLocalDbConfig() {
    return !PropertiesUtil.properties.isEmpty();
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
}
