/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.io.stats;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.schema.input.FlexibleFieldSchema;
import com.datasqrl.schema.input.FlexibleFieldSchema.FieldType;
import com.datasqrl.schema.input.FlexibleTableSchema;
import lombok.NonNull;

import java.util.List;

public interface SchemaGenerator {
  public FlexibleTableSchema mergeSchema(@NonNull SourceTableStatistics tableStats,
                                                    @NonNull FlexibleTableSchema tableDef, @NonNull ErrorCollector errors);

  public FlexibleTableSchema mergeSchema(@NonNull SourceTableStatistics tableStats,
                                                    @NonNull Name tableName, @NonNull ErrorCollector errors);

  public FlexibleFieldSchema.FieldType matchType(TypeSignature typeSignature,
                                                 List<FieldType> fieldTypes);
}
