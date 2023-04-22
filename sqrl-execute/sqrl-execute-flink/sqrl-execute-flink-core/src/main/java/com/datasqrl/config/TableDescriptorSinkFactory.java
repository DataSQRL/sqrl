package com.datasqrl.config;

import org.apache.flink.table.api.TableDescriptor;

public interface TableDescriptorSinkFactory extends SinkFactory<TableDescriptor.Builder, SinkFactoryContext> {

  @Override
  default String getEngine() {
    return "flink";
  }

}
