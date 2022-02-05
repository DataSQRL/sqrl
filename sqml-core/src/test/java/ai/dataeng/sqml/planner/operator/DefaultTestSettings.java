package ai.dataeng.sqml.planner.operator;

import ai.dataeng.sqml.config.SqrlSettings;
import ai.dataeng.sqml.execution.flink.process.FlinkConfiguration;
import ai.dataeng.sqml.execution.flink.process.FlinkGenerator;
import ai.dataeng.sqml.execution.sql.SQLConfiguration;
import ai.dataeng.sqml.execution.sql.SQLGenerator;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.impl.VertxInternal;
import io.vertx.jdbcclient.JDBCConnectOptions;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.sqlclient.PoolOptions;
import java.nio.file.Path;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;

public class DefaultTestSettings {
  private static final Path dbPath = Path.of("tmp","output");
  private static final String JDBC_URL = "jdbc:h2:"+dbPath.toAbsolutePath().toString()+";database_to_upper=false";
  private static final JdbcConnectionOptions jdbcOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
      .withUrl(JDBC_URL)
      .withDriverName("org.h2.Driver")
      .build();
  private static final FlinkConfiguration flinkConfig = new FlinkConfiguration(jdbcOptions);
  private static final SQLConfiguration sqlConfig = new SQLConfiguration(SQLConfiguration.Dialect.H2,jdbcOptions);

  public static SqrlSettings create(Vertx vertx) {
    JDBCPool pool = JDBCPool.pool(
        vertx,
        new JDBCConnectOptions()
            .setJdbcUrl(JDBC_URL),
        new PoolOptions()
            .setMaxSize(1)
    );

    return SqrlSettings.createDefault()
        .sqlGeneratorProvider(()->new SQLGenerator(sqlConfig))
        .flinkGeneratorProvider((env)-> new FlinkGenerator(flinkConfig, env))
        .sqlClientProvider(() -> pool)
        .build();
  }
}
