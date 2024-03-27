package com.datasqrl.io.file;

import com.datasqrl.io.connector.ConnectorConfig;
import com.datasqrl.io.formats.Format;
import com.datasqrl.io.tables.ConnectorFactory;

public interface FileConnectorFactory extends ConnectorFactory {

  ConnectorConfig forFiles(FilePath directory, String fileRegex, Format format);


}
