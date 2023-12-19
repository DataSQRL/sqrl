package com.datasqrl;

import com.datasqrl.config.JobListenerFactory;
import com.google.auto.service.AutoService;
import io.openlineage.flink.OpenLineageFlinkJobListener;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.core.execution.JobListener;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

@Slf4j
@AutoService(JobListenerFactory.class)
public class OpenLineageJobListenerFactory implements JobListenerFactory {

  @Override
  public JobListener create(StreamExecutionEnvironment env, Map<String, String> config) {
    JobListener listener = OpenLineageFlinkJobListener.builder()
        .executionEnvironment(env)
        .build();
    return listener;
  }


  public boolean isEnabled(Map<String, String> config) {
    String attached = config.get(DeploymentOptions.ATTACHED.key());
    String openLineage = config.get("openlineage.transport.type");
    if (openLineage != null) {
      if (attached == null || attached.equals("false")) {
        log.warn("Pipeline not executing in attached mode, lineage will not be available");
        return false;
      }
      return true;
    }
    return false;
  }
}
