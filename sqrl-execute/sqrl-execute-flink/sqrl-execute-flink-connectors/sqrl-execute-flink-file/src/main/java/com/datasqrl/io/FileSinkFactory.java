package com.datasqrl.io;

import com.datasqrl.config.FlinkSinkFactoryContext;
import com.datasqrl.config.TableDescriptorSinkFactory;
import com.datasqrl.io.impl.file.FileDataSystemConfig;
import com.datasqrl.io.impl.file.FileDataSystemFactory;
import com.datasqrl.io.impl.file.FilePathConfig;
import com.datasqrl.io.tables.TableConfig;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableDescriptor.Builder;

public class FileSinkFactory implements TableDescriptorSinkFactory {

  @Override
  public String getSinkType() {
    return FileDataSystemFactory.SYSTEM_NAME;
  }

  @Override
  public Builder create(FlinkSinkFactoryContext context) {
    TableConfig tableConfig = context.getTableConfig();
    FilePathConfig fpConfig = FileDataSystemConfig.fromConfig(tableConfig).getFilePath(tableConfig.getErrors());
    TableDescriptor.Builder tblBuilder = TableDescriptor.forConnector("filesystem")
        .option("path",
            fpConfig.getDirectory().resolve(tableConfig.getBase().getIdentifier())
                .toString())
        .format(createFormat(context.getFormatFactory(),
            context.getTableConfig().getFormatConfig()).build());
    return tblBuilder;
  }

}
