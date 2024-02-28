package com.datasqrl.io.tables;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.io.DataSystemConnectorSettings;
import com.datasqrl.io.tables.AbstractExternalTable.Digest;
import java.util.Optional;

public interface ExternalTable {

  DataSystemConnectorSettings getConnector();
  TableConfig getConfiguration();
  NamePath getPath();
  Name getName();
  Optional<TableSchema> getTableSchema();
  String qualifiedName();
  Digest getDigest();
}
