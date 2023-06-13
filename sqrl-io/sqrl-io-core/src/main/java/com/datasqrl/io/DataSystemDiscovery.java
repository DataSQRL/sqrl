/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.io;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.tables.BaseTableConfig;
import com.datasqrl.io.tables.TableConfig;
import com.datasqrl.canonicalizer.Name;
import lombok.AllArgsConstructor;
import lombok.NonNull;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;

public interface DataSystemDiscovery {

  /**
   * The name of the dataset produced by this data source if discoverable from the configuration
   *
   * @return name of dataset
   */
//  @NonNull Optional<String> getDefaultName();

  boolean requiresFormat(ExternalDataType type);

  default Collection<TableConfig> discoverSources(@NonNull ErrorCollector errors) {
    return Collections.EMPTY_LIST;
  }

  default Optional<TableConfig> discoverSink(@NonNull Name sinkName,
      @NonNull ErrorCollector errors) {
    return Optional.empty();
  }

  @AllArgsConstructor
  abstract class Base implements DataSystemDiscovery {

    protected TableConfig genericTable;

    protected TableConfig.Builder copyGeneric(@NonNull Name tableName,
        @NonNull ExternalDataType type) {
      return copyGeneric(tableName, tableName.getCanonical(), type);
    }

    protected TableConfig.Builder copyGeneric(@NonNull Name tableName,
        @NonNull String identifier, @NonNull ExternalDataType type) {
      return copyGeneric(genericTable, tableName, identifier, type);
    }

    public static TableConfig.Builder copyGeneric(@NonNull TableConfig genericTable,
        @NonNull Name tableName,
        @NonNull String identifier, @NonNull ExternalDataType type) {
      TableConfig.Builder builder = TableConfig.builder(tableName);
      builder.copyFrom(genericTable);
      BaseTableConfig base = genericTable.getBase().toBuilder()
          .type(type.name())
          .identifier(identifier)
          .build();
      builder.base(base);
      return builder;
    }

  }

}
