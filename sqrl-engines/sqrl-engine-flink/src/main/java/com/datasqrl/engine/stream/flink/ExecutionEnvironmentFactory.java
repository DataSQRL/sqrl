package com.datasqrl.engine.stream.flink;

import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ExecutionEnvironmentFactory {

  private final Map<String, String> flinkConf;

  public ExecutionEnvironmentFactory(Map<String, String> flinkConf) {
    this.flinkConf = flinkConf;
  }

  public StreamExecutionEnvironment createEnvironment() {
    Map conf = new HashMap(flinkConf);

    StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(
        org.apache.flink.configuration.Configuration.fromMap(conf));
    env.getConfig().enableObjectReuse();
    env.setRuntimeMode(RuntimeExecutionMode.STREAMING); //todo add to config
    return env;
  }
}
