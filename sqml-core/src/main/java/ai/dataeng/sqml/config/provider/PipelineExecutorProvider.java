package ai.dataeng.sqml.config.provider;

import ai.dataeng.sqml.execution.StreamExecutor;

public interface PipelineExecutorProvider {
  StreamExecutor createStreamExecutor();
}
