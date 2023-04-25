package com.datasqrl.config;

import org.apache.flink.table.api.TableDescriptor;

public interface TableDescriptorSinkFactory extends SinkFactory<TableDescriptor.Builder, FlinkSinkFactoryContext> {

  @Override
  default String getEngine() {
    return "flink";
  }

}
