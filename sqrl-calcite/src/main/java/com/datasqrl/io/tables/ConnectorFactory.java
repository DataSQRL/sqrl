package com.datasqrl.io.tables;

import com.datasqrl.config.SqrlConfig;
import com.datasqrl.io.formats.Format;
import java.util.Optional;

public interface ConnectorFactory {

  Optional<Format> getFormat(SqrlConfig connectorConfig);

  Optional<Format> getFormatForExtension(String formatExtension);

  String getConnectorName(SqrlConfig connectorConfig);

  TableType getTableType(SqrlConfig connectorConfig);

}
