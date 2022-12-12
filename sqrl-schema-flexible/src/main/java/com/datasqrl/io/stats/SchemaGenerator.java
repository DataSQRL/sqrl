package com.datasqrl.io.stats;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.name.Name;
import com.datasqrl.schema.input.FlexibleDatasetSchema;
import com.datasqrl.schema.input.FlexibleDatasetSchema.FieldType;
import java.util.List;
import lombok.NonNull;

public interface SchemaGenerator {
  public FlexibleDatasetSchema.TableField mergeSchema(@NonNull SourceTableStatistics tableStats,
      @NonNull FlexibleDatasetSchema.TableField tableDef, @NonNull ErrorCollector errors);

  public FlexibleDatasetSchema.TableField mergeSchema(@NonNull SourceTableStatistics tableStats,
      @NonNull Name tableName, @NonNull ErrorCollector errors);

  public FlexibleDatasetSchema.FieldType matchType(TypeSignature typeSignature,
      List<FieldType> fieldTypes);
}
