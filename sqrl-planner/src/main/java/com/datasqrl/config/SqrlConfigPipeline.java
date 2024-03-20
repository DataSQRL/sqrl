package com.datasqrl.config;

import static com.datasqrl.config.PipelineFactory.ENGINES_PROPERTY;

import com.datasqrl.config.PipelineFactory;
import com.datasqrl.config.SqrlConfig;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import lombok.experimental.Delegate;

@Singleton
public class SqrlConfigPipeline implements ExecutionPipeline {

  @Delegate
  ExecutionPipeline pipeline;

  @Inject
  public SqrlConfigPipeline(SqrlConfig config) {
    this.pipeline = new PipelineFactory(config.getSubConfig(ENGINES_PROPERTY))
        .createPipeline();
  }
}
