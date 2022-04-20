package ai.datasqrl.settings;

import ai.datasqrl.api.ConfigurationTest;
import ai.datasqrl.config.EnvironmentConfiguration;
import ai.datasqrl.config.GlobalConfiguration;
import ai.datasqrl.config.SqrlSettings;
import ai.datasqrl.config.engines.FlinkConfiguration;
import ai.datasqrl.config.engines.JDBCConfiguration;
import io.vertx.core.Vertx;
import io.vertx.jdbcclient.JDBCConnectOptions;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.sqlclient.PoolOptions;
import java.nio.file.Path;

/**
 * TODO: Once we have vertx folded into settings and configuration,
 * this class should be deleted in favor of {@link ConfigurationTest#getDefaultSettings()}
 */
public class DefaultTestSettings {
  private static final Path dbPath = Path.of("tmp");
  private static final String jdbcURL = "jdbc:h2:"+dbPath.toAbsolutePath();

  public static GlobalConfiguration getGlobalConfigH2(boolean monitorSources) {
    return GlobalConfiguration.builder()
            .engines(GlobalConfiguration.Engines.builder()
                    .jdbc(JDBCConfiguration.builder()
                            .dbURL(jdbcURL)
                            .driverName("org.h2.Driver")
                            .dialect(JDBCConfiguration.Dialect.H2)
                            .build())
                    .flink(new FlinkConfiguration())
                    .build())
            .environment(EnvironmentConfiguration.builder()
                    .monitorSources(monitorSources)
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
