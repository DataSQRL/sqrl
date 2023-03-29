package com.datasqrl.io;

import com.datasqrl.config.SinkFactory;
import com.datasqrl.io.formats.FormatConfiguration;
import com.datasqrl.io.impl.file.DirectoryDataSystem.DirectoryConnector;
import com.datasqrl.io.impl.file.DirectoryDataSystemConfig;
import com.datasqrl.io.tables.TableConfig;
import com.datasqrl.plan.global.PhysicalDAGPlan.ExternalSink;
import com.datasqrl.plan.global.PhysicalDAGPlan.WriteSink;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableDescriptor.Builder;

public class FileSinkFactory implements SinkFactory<TableDescriptor.Builder> {

  @Override
  public String getEngine() {
    return "flink";
  }

  @Override
  public String getSinkType() {
    return DirectoryDataSystemConfig.SYSTEM_TYPE;
  }

  @Override
  public Builder create(WriteSink sink, DataSystemConnectorConfig config) {
    ExternalSink externalSink = (ExternalSink) sink;
    TableConfig configuration = externalSink.getTableSink().getConfiguration();
    DirectoryConnector connector = (DirectoryConnector)externalSink.getTableSink().getConnector();
    TableDescriptor.Builder tblBuilder = TableDescriptor.forConnector("filesystem")
        .option("path",
            connector.getPathConfig().getDirectory().resolve(configuration.getIdentifier())
                .toString());
    addFormat(tblBuilder, configuration.getFormat());
    return tblBuilder;
  }

  private void addFormat(TableDescriptor.Builder tblBuilder, FormatConfiguration formatConfig) {
    switch (formatConfig.getFileFormat()) {
      case CSV:
        tblBuilder.format("csv");
        break;
      case JSON:
        tblBuilder.format("json");
        break;
      default:
        throw new UnsupportedOperationException(
            "Unsupported format: " + formatConfig.getFileFormat());
    }
  }
}
