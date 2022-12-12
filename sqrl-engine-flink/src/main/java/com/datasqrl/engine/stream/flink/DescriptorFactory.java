package com.datasqrl.engine.stream.flink;

import com.datasqrl.Descriptor;
import com.datasqrl.DirectoryDescriptor;
import com.datasqrl.PrintDescriptor;
import com.datasqrl.Sink;
import com.datasqrl.io.DataSystemConnector;
import com.datasqrl.io.impl.file.DirectoryDataSystem;
import com.datasqrl.io.impl.print.PrintDataSystem;
import com.datasqrl.io.tables.TableConfig;
import com.datasqrl.plan.global.OptimizedDAG;
import com.datasqrl.plan.global.OptimizedDAG.WriteSink;
import java.util.HashMap;
import java.util.Map;
import lombok.NonNull;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;

public class DescriptorFactory {
  Map<Class, Descriptor> descriptors = new HashMap<>();
  Map<Class, Sink> sinks = new HashMap<>();
  public DescriptorFactory() {
    descriptors.put(PrintDataSystem.Connector.class, new PrintDescriptor());
    descriptors.put(DirectoryDataSystem.Connector.class, new DirectoryDescriptor());
    sinks.put(OptimizedDAG.DatabaseSink.class, new DBSink());
  }

  public TableDescriptor createDescriptor(String name, Schema tblSchema, DataSystemConnector connector,
      @NonNull TableConfig configuration) {
    return descriptors.get(connector.getClass()).create(name, tblSchema, connector, configuration);
  }

  public TableDescriptor createDescriptor(WriteSink sink, Schema tblSchema) {
    return sinks.get(sink.getClass()).create(
        sink, tblSchema);
  }
}
