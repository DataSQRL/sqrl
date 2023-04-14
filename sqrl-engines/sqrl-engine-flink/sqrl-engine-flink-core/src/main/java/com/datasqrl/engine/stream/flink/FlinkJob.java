package com.datasqrl.engine.stream.flink;

import com.datasqrl.engine.stream.monitor.DataMonitor;
import com.datasqrl.engine.stream.monitor.DataMonitor.Job;
import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

@Slf4j
public class FlinkJob implements DataMonitor.Job {

  private final StreamExecutionEnvironment execEnv;

  private Status status = Status.PREPARING;
  private JobClient jobClient = null;

  protected FlinkJob(StreamExecutionEnvironment execEnv) {
    this.execEnv = execEnv;
  }

  @Override
  public void execute(String name) {
    status = Status.RUNNING;
    try {
      JobExecutionResult result = execEnv.execute(name);
      status = Status.COMPLETED;
    } catch (Exception e) {
      status = Status.FAILED;
      throw new RuntimeException(e);
    }
  }

  @Override
  public void executeAsync(String name) {
    try {
      status = Status.RUNNING;
      this.jobClient = execEnv.executeAsync(name);
    } catch (Exception e) {
      status = Status.FAILED;
      throw new RuntimeException(e);
    }
  }

  @Override
  public void cancel() {
    Preconditions.checkArgument(status==Status.RUNNING, "Only running jobs can be canceled");
    try {
      jobClient.cancel().get();
      status = Status.COMPLETED;
    } catch (Exception e) {
    }
  }

  @Override
  public Status getStatus() {
    if (jobClient == null)
      return status;
    try {
      JobStatus flinkStatus = jobClient.getJobStatus().get();
      status = map(flinkStatus);
    } catch (IllegalStateException e) {
      //Thrown when minicluster is already shut down - we assume success
      if (status==Status.RUNNING) status = Status.COMPLETED;
    } catch (Exception e) {
      log.error("Encountered job runner exception", e);
      status = Status.FAILED;
    }
    return status;
  }

  private static Job.Status map(JobStatus status) {
    switch (status) {
      case INITIALIZING:
      case RUNNING:
      case CREATED:
      case CANCELLING:
      case RESTARTING:
      case RECONCILING:
        return Status.RUNNING;
      case CANCELED:
      case FINISHED:
        return Status.COMPLETED;
      case FAILING:
      case FAILED:
      default:
        return Status.FAILED;
    }
  }

}
