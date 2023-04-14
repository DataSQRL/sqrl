package com.datasqrl.io;

import com.datasqrl.config.TableDescriptorSinkFactory;
import com.datasqrl.io.impl.print.PrintDataSystem;
import com.datasqrl.io.tables.TableConfig;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableDescriptor.Builder;

public class PrintSinkFactory implements TableDescriptorSinkFactory {

  @Override
  public String getEngine() {
    return "flink";
  }

  @Override
  public String getSinkType() {
    return PrintDataSystem.SYSTEM_TYPE;
  }

  @Override
  public Builder create(FlinkSinkFactoryContext context) {
    TableConfig tblConfig = context.getTableConfig();
    PrintDataSystem.Connector printConnector = (PrintDataSystem.Connector)context.getConfig().initialize(context.getErrors());
    String identifier = printConnector.getPrefix() + tblConfig.getName();
    return TableDescriptor.forConnector("print")
        .option("print-identifier", identifier);
  }
}
