package com.datasqrl.config;

import java.util.Map;
import org.apache.flink.core.execution.JobListener;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public interface JobListenerFactory {

  JobListener create(StreamExecutionEnvironment env,  Map<String, String> config);
  boolean isEnabled(Map<String, String> config);

}
