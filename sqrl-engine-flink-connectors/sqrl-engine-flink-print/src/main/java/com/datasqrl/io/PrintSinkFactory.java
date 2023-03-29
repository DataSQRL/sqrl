package com.datasqrl.io;

import com.datasqrl.config.SinkFactory;
import com.datasqrl.io.impl.print.PrintDataSystem;
import com.datasqrl.io.tables.TableConfig;
import com.datasqrl.plan.global.PhysicalDAGPlan;
import com.datasqrl.plan.global.PhysicalDAGPlan.WriteSink;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableDescriptor.Builder;

public class PrintSinkFactory implements SinkFactory<TableDescriptor.Builder> {

  @Override
  public String getEngine() {
    return "flink";
  }

  @Override
  public String getSinkType() {
    return PrintDataSystem.SYSTEM_TYPE;
  }

  @Override
  public Builder create(WriteSink sink, DataSystemConnectorConfig config) {
    PhysicalDAGPlan.ExternalSink externalSink = (PhysicalDAGPlan.ExternalSink) sink;
    TableConfig tblConfig = externalSink.getTableSink().getConfiguration();
    assert config instanceof PrintDataSystem.Connector;
    PrintDataSystem.Connector printConfig = (PrintDataSystem.Connector) config;
    String identifier = printConfig.getPrefix() + tblConfig.getName();
    return TableDescriptor.forConnector("print")
        .option("print-identifier", identifier);
  }
}
