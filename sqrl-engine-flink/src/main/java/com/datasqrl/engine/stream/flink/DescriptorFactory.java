/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.stream.flink;

import com.datasqrl.config.SinkFactory;
import com.datasqrl.config.SinkServiceLoader;
import com.datasqrl.engine.stream.flink.plan.FlinkPipelineUtils;
import com.datasqrl.io.DataSystemConnectorConfig;
import com.datasqrl.plan.global.OptimizedDAG.EngineSink;
import com.datasqrl.plan.global.OptimizedDAG.ExternalSink;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;

public class DescriptorFactory {
  public TableDescriptor createSink(ExternalSink sink, Schema tblSchema) {
    DataSystemConnectorConfig config = sink.getSink().getConfiguration().getConnector();

    SinkFactory<TableDescriptor.Builder> factory = (new SinkServiceLoader())
        .load(FlinkEngineConfiguration.ENGINE_NAME,
            sink.getSink().getConnector().getPrefix())
        .orElseThrow();

    return factory.create(sink, config)
        .schema(tblSchema)
        .build();
  }

  public TableDescriptor createSink(EngineSink sink, Schema tblSchema) {
    DataSystemConnectorConfig config = sink.getStage().getEngine().getDataSystemConnectorConfig();
    SinkFactory<TableDescriptor.Builder> factory = (new SinkServiceLoader())
        .load(FlinkEngineConfiguration.ENGINE_NAME,
            sink.getStage().getName())
          .orElseThrow();
    tblSchema = FlinkPipelineUtils.addPrimaryKey(tblSchema, sink);

    return factory.create(sink, config)
        .schema(tblSchema)
        .build();
  }
}
