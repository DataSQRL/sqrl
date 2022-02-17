package ai.dataeng.sqml.planner.operator;

import ai.dataeng.sqml.config.EnvironmentConfiguration;
import ai.dataeng.sqml.config.GlobalConfiguration;
import ai.dataeng.sqml.config.SqrlSettings;
import ai.dataeng.sqml.config.engines.JDBCConfiguration;
import io.vertx.core.Vertx;
import io.vertx.jdbcclient.JDBCConnectOptions;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.sqlclient.PoolOptions;
import java.nio.file.Path;

public class DefaultTestSettings {
  private static final Path dbPath = Path.of("tmp");
  private static final String jdbcURL = "jdbc:h2:"+dbPath.toAbsolutePath().toString();

  public static GlobalConfiguration getGlobalConfigH2(boolean monitorSources) {
    return GlobalConfiguration.builder()
            .engines(GlobalConfiguration.Engines.builder()
                    .jdbc(JDBCConfiguration.builder()
                            .dbURL(jdbcURL)
                            .driverName("org.h2.Driver")
                            .dialect(JDBCConfiguration.Dialect.H2)
                            .build())
                    .build())
            .environment(EnvironmentConfiguration.builder()
                    .monitor_sources(monitorSources)
                    .build())
            .build();
  }

  public static SqrlSettings create(Vertx vertx) {
    //TODO: this is hardcoded for now and needs to be integrated into configuration
    JDBCPool pool = JDBCPool.pool(
        vertx,
        new JDBCConnectOptions()
            .setJdbcUrl("jdbc:h2:"+dbPath.toAbsolutePath().toString()+"/c360" + ";database_to_upper=false"),
        new PoolOptions()
            .setMaxSize(1)
    );

    return SqrlSettings.builderFromConfiguration(getGlobalConfigH2(false))
        .sqlClientProvider(() -> pool)
        .build();
  }
}
