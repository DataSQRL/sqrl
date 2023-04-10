package com.datasqrl.config;

import com.datasqrl.config.SinkFactory.FlinkSinkFactoryContext;
import org.apache.flink.table.api.TableDescriptor;

public interface TableDescriptorSinkFactory extends SinkFactory<TableDescriptor.Builder, FlinkSinkFactoryContext> {

}
