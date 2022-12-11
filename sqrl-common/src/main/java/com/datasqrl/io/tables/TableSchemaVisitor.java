package com.datasqrl.io.tables;

import com.datasqrl.schema.input.FlexibleDatasetSchema.TableField;
import com.datasqrl.schema.input.JsonTableSchema;

public interface TableSchemaVisitor<R, C> {

  R accept(TableField tableField, C context);

  R accept(JsonTableSchema jsonTableSchema, C context);
}
