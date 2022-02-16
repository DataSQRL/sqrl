package ai.dataeng.sqml.execution.flink.environment;

import ai.dataeng.sqml.execution.StreamEngine;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public interface FlinkStreamEngine extends StreamEngine {

  public StreamExecutionEnvironment create();

}
