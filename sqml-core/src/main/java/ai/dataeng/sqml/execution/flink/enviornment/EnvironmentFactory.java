package ai.dataeng.sqml.execution.flink.enviornment;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public interface EnvironmentFactory {

  public StreamExecutionEnvironment create();
}
