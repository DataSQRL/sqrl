package ai.dataeng.sqml.execution;

public interface StreamExecutor {

  void register(ExecutionPlan executionPlan);

  void execute();

  void stop();
}
