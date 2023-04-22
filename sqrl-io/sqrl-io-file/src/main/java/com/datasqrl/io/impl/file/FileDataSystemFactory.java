package com.datasqrl.io.impl.file;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.config.SqrlConfig;
import com.datasqrl.io.formats.FormatFactory;
import com.datasqrl.io.formats.JsonLineFormat;
import com.datasqrl.io.tables.BaseTableConfig;
import com.datasqrl.io.DataSystemConnector;
import com.datasqrl.io.DataSystemConnectorFactory;
import com.datasqrl.io.DataSystemDiscovery;
import com.datasqrl.io.DataSystemDiscoveryFactory;
import com.datasqrl.io.DataSystemImplementationFactory;
import com.datasqrl.io.ExternalDataType;
import com.datasqrl.io.tables.TableConfig;
import com.google.auto.service.AutoService;
import com.google.common.base.Preconditions;
import java.nio.file.Files;
import java.nio.file.Path;
import lombok.NonNull;

public class FileDataSystemFactory implements DataSystemImplementationFactory {

  public static final String SYSTEM_NAME = "file";

  @Override
  public String getSystemName() {
    return SYSTEM_NAME;
  }

  public static TableConfig.Builder getFileDiscoveryConfig(String name, FileDataSystemConfig config) {
    BaseTableConfig baseTable = BaseTableConfig.builder()
        .type(ExternalDataType.source_and_sink.name())
        .build();
    TableConfig.Builder tblBuilder = TableConfig.builder(Name.system(name)).base(baseTable);
    SqrlConfig connectorConfig = tblBuilder.getConnectorConfig();
    connectorConfig.setProperty(SYSTEM_NAME_KEY, SYSTEM_NAME);
    connectorConfig.setProperties(config);
    return tblBuilder;
  }

  public static TableConfig.Builder getFileDiscoveryConfig(Path path) {
    Preconditions.checkArgument(Files.isDirectory(path));
    String name = path.getFileName().toString();
    return getFileDiscoveryConfig(name,
        FileDataSystemConfig.builder().directoryURI(path.toString()).build());
  }

  public static TableConfig.Builder getFileSinkConfig(Path path) {
    TableConfig.Builder builder = getFileDiscoveryConfig(path);
    builder.getFormatConfig().setProperty(FormatFactory.FORMAT_NAME_KEY, JsonLineFormat.NAME);
    return builder;
  }


  @AutoService(DataSystemConnectorFactory.class)
  public static class Connector extends FileDataSystemFactory
      implements DataSystemConnectorFactory {

    @Override
    public DataSystemConnector initialize(@NonNull SqrlConfig connectorConfig) {
      return new FileDataSystemConnector();
    }

  }

  @AutoService(DataSystemDiscoveryFactory.class)
  public static class Discovery extends FileDataSystemFactory implements
      DataSystemDiscoveryFactory {

    @Override
    public DataSystemDiscovery initialize(@NonNull TableConfig tableConfig) {
      FileDataSystemConfig fileConfig = FileDataSystemConfig.fromConfig(tableConfig);
      return new FileDataSystemDiscovery(tableConfig, fileConfig.getFilePath(tableConfig.getErrors()),
          fileConfig.getPattern());
    }


  }

}
