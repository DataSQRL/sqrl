package ai.dataeng.sqml.execution.flink.environment;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public interface EnvironmentFactory {

  public StreamExecutionEnvironment create();
}
