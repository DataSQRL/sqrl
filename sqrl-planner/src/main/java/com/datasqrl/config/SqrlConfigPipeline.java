package com.datasqrl.config;

import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import lombok.experimental.Delegate;

@Singleton
public class SqrlConfigPipeline implements ExecutionPipeline {

  @Delegate
  ExecutionPipeline pipeline;

  @Inject
  public SqrlConfigPipeline(PackageJson config, ConnectorFactoryFactory connectorFactory) {
    this.pipeline = new PipelineFactory(config.getPipeline(), config.getEngines(), connectorFactory)
        .createPipeline();
  }
}
