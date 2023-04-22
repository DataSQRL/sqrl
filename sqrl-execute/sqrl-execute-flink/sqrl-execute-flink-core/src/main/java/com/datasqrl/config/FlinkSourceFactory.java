package com.datasqrl.config;

public interface FlinkSourceFactory<ENGINE_SOURCE> extends SourceFactory<ENGINE_SOURCE, FlinkSourceFactoryContext> {

  @Override
  default String getEngine() {
    return "flink";
  }

}
