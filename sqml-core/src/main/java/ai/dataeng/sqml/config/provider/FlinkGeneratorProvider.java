package ai.dataeng.sqml.config.provider;

import ai.dataeng.sqml.execution.flink.environment.FlinkStreamEngine;
import ai.dataeng.sqml.execution.flink.process.FlinkGenerator;

public interface FlinkGeneratorProvider {
  FlinkGenerator create(FlinkStreamEngine envProvider);
}
