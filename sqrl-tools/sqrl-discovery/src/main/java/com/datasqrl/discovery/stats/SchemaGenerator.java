/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.discovery.stats;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.io.schema.flexible.input.FlexibleTableSchema;
import lombok.NonNull;

public interface SchemaGenerator {
  public FlexibleTableSchema mergeSchema(@NonNull SourceTableStatistics tableStats,
                                                    @NonNull FlexibleTableSchema tableDef, @NonNull ErrorCollector errors);

  public FlexibleTableSchema mergeSchema(@NonNull SourceTableStatistics tableStats,
                                                    @NonNull Name tableName, @NonNull ErrorCollector errors);
}
