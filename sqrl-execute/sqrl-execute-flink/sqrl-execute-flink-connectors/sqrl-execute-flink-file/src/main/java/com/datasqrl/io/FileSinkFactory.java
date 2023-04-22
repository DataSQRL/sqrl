package com.datasqrl.io;

import com.datasqrl.config.SinkFactoryContext;
import com.datasqrl.config.SinkFactoryContext.Implementation;
import com.datasqrl.config.TableDescriptorSinkFactory;
import com.datasqrl.io.formats.FormatFactory;
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
  public Builder create(SinkFactoryContext context) {
    TableConfig tableConfig = context.getTableConfig();
    FilePathConfig fpConfig = FileDataSystemConfig.fromConfig(tableConfig).getFilePath(tableConfig.getErrors());
    TableDescriptor.Builder tblBuilder = TableDescriptor.forConnector("filesystem")
        .option("path",
            fpConfig.getDirectory().resolve(tableConfig.getBase().getIdentifier())
                .toString());
    addFormat(tblBuilder, tableConfig.getFormat());
    return tblBuilder;
  }

  private void addFormat(TableDescriptor.Builder tblBuilder, FormatFactory format) {
    tblBuilder.format(format.getName());
  }
}
