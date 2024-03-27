package com.datasqrl.discovery.system;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.io.ExternalDataType;
import com.datasqrl.io.formats.Format;
import com.datasqrl.io.formats.Format.DefaultFormat;
import com.datasqrl.io.file.FileFlinkConnectorFactory;
import com.datasqrl.io.file.FilePath;
import com.datasqrl.io.tables.TableConfig;
import lombok.Getter;

public class FileTableConfigFactory { //todo: move to io-core

  public static final String DEFAULT_INGEST_TIME = "_ingest_time";
  public static final String DEFAULT_UUID = "_uuid";

  @Getter
  private final FileFlinkConnectorFactory connectorFactory = new FileFlinkConnectorFactory();

  public TableConfig.Builder forDiscovery(Name tblName, FilePath directory,
      String fileRegex, Format format) {
    TableConfig.Builder builder = TableConfig.builder(tblName);
    builder.setType(ExternalDataType.source);
    builder.addUuid(DEFAULT_UUID);
    builder.setPrimaryKey(new String[]{DEFAULT_UUID});
    builder.addIngestTime(DEFAULT_INGEST_TIME);
    builder.setTimestampColumn(DEFAULT_INGEST_TIME);
    builder.setWatermark(1);
    builder.copyConnectorConfig(connectorFactory.forFiles(directory, fileRegex, format));
    return builder;
  }

  public TableConfig.Builder forMock(String name) {
    return forDiscovery(Name.system(name), new FilePath("file:///mock"),
        "", new DefaultFormat("json"));
  }

}
