/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.stream.flink;

import com.datasqrl.Descriptor;
import com.datasqrl.DirectoryDescriptor;
import com.datasqrl.PrintDescriptor;
import com.datasqrl.Sink;
import com.datasqrl.config.SinkFactory;
import com.datasqrl.config.SinkServiceLoader;
import com.datasqrl.engine.stream.flink.plan.FlinkPipelineUtils;
import com.datasqrl.io.DataSystemConnector;
import com.datasqrl.io.impl.file.DirectoryDataSystem;
import com.datasqrl.io.impl.print.PrintDataSystem;
import com.datasqrl.io.tables.TableConfig;
import com.datasqrl.plan.global.OptimizedDAG.EngineSink;
import com.datasqrl.plan.global.OptimizedDAG.WriteSink;
import java.util.HashMap;
import java.util.Map;
import lombok.NonNull;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;

public class DescriptorFactory {
  Map<Class, Descriptor> descriptors = new HashMap<>();
  public DescriptorFactory() {
    descriptors.put(PrintDataSystem.Connector.class, new PrintDescriptor());
    descriptors.put(DirectoryDataSystem.Connector.class, new DirectoryDescriptor());
  }

  public TableDescriptor createDescriptor(String name, Schema tblSchema, DataSystemConnector connector,
      @NonNull TableConfig configuration) {
    return descriptors.get(connector.getClass()).create(name, tblSchema, connector, configuration);
  }

  public TableDescriptor createDescriptor(EngineSink sink, Schema tblSchema) {
    SinkFactory<TableDescriptor.Builder> factory = (new SinkServiceLoader())
        .load(FlinkEngineConfiguration.ENGINE_NAME,
            sink.getStage().getName())
          .orElseThrow();
    tblSchema = FlinkPipelineUtils.addPrimaryKey(tblSchema, sink);

    return factory.create(sink)
        .schema(tblSchema)
        .option("table-name", sink.getName())
        .build();
  }
}
