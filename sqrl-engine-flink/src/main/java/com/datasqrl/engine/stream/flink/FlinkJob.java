package com.datasqrl.engine.stream.flink;

import com.datasqrl.engine.stream.monitor.DataMonitor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

@Slf4j
public class FlinkJob implements DataMonitor.Job {

  private final StreamExecutionEnvironment execEnv;
  protected Status status = Status.PREPARING;

  protected FlinkJob(StreamExecutionEnvironment execEnv) {
    this.execEnv = execEnv;
  }

  @Override
  public void execute(String name) {
    try {
      //TODO: move to async execution
      JobExecutionResult result = execEnv.execute(name);
    } catch (Exception e) {
      status = Status.FAILED;
      throw new RuntimeException(e);
//        log.error("Failed to launch Flink job",e);
    }
    status = Status.RUNNING;
  }

  @Override
  public void cancel() {
    //TODO
    status = Status.COMPLETED;
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  public Status getStatus() {
    return status;
  }
}
