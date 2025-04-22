package com.datasqrl.io.tables;

import java.util.Optional;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.config.TableConfig;
import com.datasqrl.io.tables.AbstractExternalTable.Digest;

public interface ExternalTable {

  TableConfig getConfiguration();
  NamePath getPath();
  Name getName();
  Optional<TableSchema> getTableSchema();
  String qualifiedName();
  Digest getDigest();
}
