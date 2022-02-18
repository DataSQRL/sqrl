package ai.dataeng.sqml.execution.flink.environment;

import ai.dataeng.sqml.config.provider.JDBCConnectionProvider;
import ai.dataeng.sqml.execution.StreamEngine;
import ai.dataeng.sqml.io.sources.dataset.SourceTable;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public interface FlinkStreamEngine extends StreamEngine {

  StreamExecutionEnvironment create();

  static JdbcConnectionOptions getFlinkJDBC(JDBCConnectionProvider jdbc) {
    return new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
            .withUrl(jdbc.getDbURL())
            .withDriverName(jdbc.getDriverName())
            .withUsername(jdbc.getUser())
            .withPassword(jdbc.getPassword())
            .build();
  }

  @AllArgsConstructor
  @Getter
  enum JobType {
    MONITOR("monitor"), SCRIPT("script");

    private final String name;

    public String toString() {return name;}
  }

  static String getFlinkName(String type, String identifier) {
    return type + "[" + identifier + "]";
  }

  @Slf4j
  public static class Job implements StreamEngine.Job {

    private final StreamExecutionEnvironment execEnv;
    private final JobType type;
    private final String name;
    private Status status = Status.PREPARING;

    public Job(StreamExecutionEnvironment execEnv, JobType type, String identifier) {
      this.execEnv = execEnv;
      this.type = type;
      this.name = getFlinkName(type.getName(),identifier);
    }

    @Override
    public String getId() {
      return name;
    }

    @Override
    public void execute() {
      try {
        execEnv.execute(name);
      } catch (Exception e) {
        log.error("Failed to launch Flink job",e);
        status = Status.FAILED;
      }
      status = Status.RUNNING;
    }

    @Override
    public void cancel() {
      //TODO
      status = Status.STOPPED;
      throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public Status getStatus() {
      return status;
    }
  }

}
