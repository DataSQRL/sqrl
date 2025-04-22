package com.datasqrl.config;

import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Singleton;

import lombok.experimental.Delegate;

@Singleton
public class SqrlConfigPipeline implements ExecutionPipeline {

  @Delegate
  ExecutionPipeline pipeline;

  @Inject
  public SqrlConfigPipeline(Injector injector, PackageJson config) {
    this.pipeline = new PipelineFactory(injector, config.getEnabledEngines(), config.getEngines())
        .createPipeline();
  }
}
