package com.datasqrl.util;

import com.datasqrl.IntegrationTestSettings;
import com.datasqrl.config.provider.Dialect;
import com.datasqrl.engine.database.relational.JDBCEngineConfiguration;
import com.google.common.base.Preconditions;
import lombok.Getter;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.Properties;

public class JDBCTestDatabase implements DatabaseHandle {

  public static final String TEST_DATABSE_NAME = "datasqrl";

  private final Properties properties;
  @Getter
  private final PostgreSQLContainer postgreSQLContainer;

  public JDBCTestDatabase(IntegrationTestSettings.DatabaseEngine dbType) {
    if (dbType == IntegrationTestSettings.DatabaseEngine.LOCAL) {
      Preconditions.checkArgument(hasLocalDbConfig());
      this.properties = PropertiesUtil.properties;
      this.postgreSQLContainer = null;
    } else if (dbType == IntegrationTestSettings.DatabaseEngine.POSTGRES){
      DockerImageName image = DockerImageName.parse("postgres:14.2");
      postgreSQLContainer = new PostgreSQLContainer(image)
          .withDatabaseName(TEST_DATABSE_NAME);
      postgreSQLContainer.start();

      this.properties = new Properties();
      this.properties.put("db.host", postgreSQLContainer.getHost());
      this.properties.put("db.port", postgreSQLContainer.getMappedPort(5432));
      this.properties.put("db.dialect", Dialect.POSTGRES.toString());
      this.properties.put("db.driverClassName", "org.postgresql.Driver");
      this.properties.put("db.username", "test");
      this.properties.put("db.password", "test");
      this.properties.put("db.url", postgreSQLContainer.getJdbcUrl().substring(0,
          postgreSQLContainer.getJdbcUrl().lastIndexOf("/")));
    } else throw new UnsupportedOperationException("Not a supported db type: " + dbType);
  }

  private static boolean hasLocalDbConfig() {
    return !PropertiesUtil.properties.isEmpty();
  }

  public JDBCEngineConfiguration getJdbcConfiguration() {
    return JDBCEngineConfiguration.builder()
        .host((String)properties.get("db.host"))
        .port((Integer)properties.get("db.port"))
        .dbURL((String)properties.get("db.url"))
        .driverName((String)properties.get("db.driverClassName"))
        .dialect(Dialect.valueOf((String)properties.get("db.dialect")))
        .user((String)properties.get("db.username"))
        .password((String)properties.get("db.password"))
        .database(TEST_DATABSE_NAME)
        .build();
  }

  @Override
  public void cleanUp() {
    if (postgreSQLContainer!=null) {
      postgreSQLContainer.stop();
    }
  }
}
