package com.datasqrl;

import com.datasqrl.io.formats.FormatConfiguration;
import com.datasqrl.io.impl.file.DirectoryDataSystem;
import com.datasqrl.io.impl.file.DirectoryDataSystem.Connector;
import com.datasqrl.io.tables.TableConfig;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;

public class DirectoryDescriptor implements Descriptor<DirectoryDataSystem.Connector> {

  @Override
  public TableDescriptor create(String name, Schema schema, Connector connector,
      TableConfig configuration) {
    TableDescriptor.Builder tblBuilder = TableDescriptor.forConnector("filesystem")
        .schema(schema)
        .option("path",
            connector.getPathConfig().getDirectory().resolve(configuration.getIdentifier())
                .toString());
    addFormat(tblBuilder, configuration.getFormat());
    return tblBuilder.build();
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
