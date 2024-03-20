package com.datasqrl.io.impl.file;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.config.SqrlConfig;
import com.datasqrl.io.DynamicSinkConnectorFactory;
import com.datasqrl.io.StandardDynamicSinkFactory;
import com.datasqrl.io.connector.ConnectorConfig;
import com.datasqrl.io.tables.FlinkConnectorFactory;
import com.google.auto.service.AutoService;
import java.nio.file.Path;
import lombok.NonNull;

//todo: move to io-core
@AutoService(DynamicSinkConnectorFactory.class)
public class FileFlinkDynamicSinkConnectorFactory extends FlinkConnectorFactory implements
    DynamicSinkConnectorFactory {

  @Override
  public ConnectorConfig forName(@NonNull Name name, @NonNull SqrlConfig baseConnectorConfig) {
    SqrlConfig connector = SqrlConfig.create(baseConnectorConfig);
    connector.setProperty(CONNECTOR_KEY, FileFlinkConnectorFactory.CONNECTOR_TYPE);
    connector.setProperty(FileFlinkConnectorFactory.PATH_KEY,
        appendDirectory(baseConnectorConfig.asString(FileFlinkConnectorFactory.PATH_KEY).get(), name.getCanonical()));
    return new ConnectorConfig(connector,this);
  }

  public static String appendDirectory(String base, String directory) {
    String baseStr = base;
    if (!baseStr.endsWith("/")) baseStr += baseStr + "/";
    return baseStr + directory;
  }

  @Override
  public String getType() {
    return FileFlinkConnectorFactory.CONNECTOR_TYPE;
  }

  /**
   * This method is used for testing only
   * @param path
   * @return
   */
  public static StandardDynamicSinkFactory forPath(Path path) {
    SqrlConfig connector = SqrlConfig.createCurrentVersion();
    connector.setProperty(CONNECTOR_KEY, FileFlinkConnectorFactory.CONNECTOR_TYPE);
    connector.setProperty(FORMAT_KEY, "json");
    connector.setProperty(FileFlinkConnectorFactory.PATH_KEY, FilePath.fromJavaPath(path).toString());
    return new StandardDynamicSinkFactory(new FileFlinkDynamicSinkConnectorFactory(), connector);
  }

}
