package com.datasqrl.io.impl.file;

import com.datasqrl.config.SqrlConfig;
import com.datasqrl.io.connector.ConnectorConfig;
import com.datasqrl.io.formats.Format;
import com.datasqrl.io.tables.FlinkConnectorFactory;
import org.apache.flink.util.FileUtils;

//todo: move to io-core
public class FileFlinkConnectorFactory extends FlinkConnectorFactory implements FileConnectorFactory {

  public static final int DEFAULT_MONITORING_INTERVAL_MS = 10000;

  public static final String CONNECTOR_TYPE = "filesystem";
  public static final String PATH_KEY = "path";
  public static final String REGEX_KEY = "source.path.regex-pattern";

  public static final String MONITOR_INTERVAL_KEY = "source.monitor-interval";

  public ConnectorConfig forFiles(FilePath directory, String fileRegex, Format format) {
    SqrlConfig connector = SqrlConfig.createCurrentVersion();
    connector.setProperty(CONNECTOR_KEY, CONNECTOR_TYPE);
    connector.setProperty(PATH_KEY, directory.toString());
    connector.setProperty(REGEX_KEY, fileRegex);
    super.setFormat(connector, format);
    updateMonitorInterval(connector, DEFAULT_MONITORING_INTERVAL_MS);
    return new ConnectorConfig(connector, this);
  }

  private void updateMonitorInterval(SqrlConfig config, int interval_milliseconds) {
    config.setProperty(MONITOR_INTERVAL_KEY, interval_milliseconds);
  }

}
