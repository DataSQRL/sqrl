package com.datasqrl.io;

import com.datasqrl.config.FlinkSinkFactoryContext;
import com.datasqrl.config.TableDescriptorSinkFactory;
import com.datasqrl.io.impl.print.PrintDataSystem;
import com.datasqrl.io.impl.print.PrintDataSystemFactory;
import com.datasqrl.io.tables.TableConfig;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableDescriptor.Builder;

public class PrintSinkFactory implements TableDescriptorSinkFactory {

  @Override
  public String getSinkType() {
    return PrintDataSystemFactory.SYSTEM_NAME;
  }

  @Override
  public Builder create(FlinkSinkFactoryContext context) {
    TableConfig tblConfig = context.getTableConfig();
    String identifier = tblConfig.getConnectorConfig().asString(PrintDataSystem.PREFIX_KEY).withDefault("").get();
    return TableDescriptor.forConnector("print")
        .option("print-identifier", identifier);
  }
}
