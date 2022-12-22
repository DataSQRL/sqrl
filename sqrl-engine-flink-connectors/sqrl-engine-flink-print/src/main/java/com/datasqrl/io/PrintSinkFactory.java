package com.datasqrl.io;

import com.datasqrl.config.SinkFactory;
import com.datasqrl.plan.global.OptimizedDAG.WriteSink;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableDescriptor.Builder;

public class PrintSinkFactory implements SinkFactory<TableDescriptor.Builder> {

  @Override
  public String getEngine() {
    return "flink";
  }

  @Override
  public String getSinkName() {
    return "print";
  }

  @Override
  public Builder create(WriteSink sink, DataSystemConnectorConfig config) {
    return TableDescriptor.forConnector("print")
        .option("print-identifier", sink.getName());
  }
}
