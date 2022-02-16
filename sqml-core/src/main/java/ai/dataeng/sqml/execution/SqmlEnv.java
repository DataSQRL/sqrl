package ai.dataeng.sqml.execution;

import ai.dataeng.sqml.execution.flink.environment.DefaultFlinkStreamEngine;
import ai.dataeng.sqml.execution.flink.environment.FlinkStreamEngine;
import ai.dataeng.sqml.io.sources.dataset.DatasetRegistry;
import java.nio.file.Path;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.internal.connection.SimpleJdbcConnectionProvider;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
   * The sqml env is the Flink environment & other persistance mechanisms
   */
@Deprecated
public class SqmlEnv {
  private final DatasetRegistry ddRegistry;
  private final SimpleJdbcConnectionProvider connectionProvider;

  //Todo make into a executor env
  private final StreamExecutionEnvironment flinkEnv;
  private final StreamTableEnvironment tableEnv;

  public static final Path outputBase = Path.of("tmp","datasource");
  public static final Path dbPath = Path.of("tmp","output");

  public static final Path RETAIL_DIR = Path.of("/Users/henneberger/Projects/sqml-official/sqml-examples/retail/");
  public static final String RETAIL_DATA_DIR_NAME = "ecommerce-data";
  public static final Path RETAIL_DATA_DIR = RETAIL_DIR.resolve(RETAIL_DATA_DIR_NAME);

  private static final JdbcConnectionOptions jdbcOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
      .withUrl("jdbc:h2:"+dbPath.toAbsolutePath().toString()+";database_to_upper=false")
      .withDriverName("org.h2.Driver")
      .build();

    public SqmlEnv(DatasetRegistry ddRegistry) {
      //    Long checkpointInterval = 1000L;
//    Long checkpointTimeout = 15*1000L;
//
//    CheckpointConfig checkpointConfig = flinkEnv.getCheckpointConfig();
//    checkpointConfig.setCheckpointTimeout(checkpointTimeout);
//    checkpointConfig.setCheckpointStorage("file://" + dbPath.toAbsolutePath().toString());
//    checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//    flinkEnv.enableCheckpointing(checkpointInterval);



      this.ddRegistry = ddRegistry;
      SimpleJdbcConnectionProvider p = new SimpleJdbcConnectionProvider(jdbcOptions);
      this.connectionProvider = p;

      FlinkStreamEngine envProvider = new DefaultFlinkStreamEngine();
      StreamExecutionEnvironment flinkEnv = envProvider.create();
      StreamTableEnvironment tableEnv = StreamTableEnvironment.create(flinkEnv);
      this.flinkEnv = flinkEnv;
      this.tableEnv = tableEnv;
    }

    public DatasetRegistry getDdRegistry() {
      return ddRegistry;
    }

    public SimpleJdbcConnectionProvider getConnectionProvider() {
      return connectionProvider;
    }

    public StreamExecutionEnvironment getFlinkEnv() {
      return flinkEnv;
    }

    public StreamTableEnvironment getTableEnv() {
      return tableEnv;
    }

    public JdbcConnectionOptions getJdbcOptions() {
      return jdbcOptions;
    }
  }