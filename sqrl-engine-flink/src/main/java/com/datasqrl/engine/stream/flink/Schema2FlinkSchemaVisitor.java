package com.datasqrl.engine.stream.flink;

import static com.datasqrl.engine.stream.flink.Schema2StreamVisitor.getUniversalTableBuilder;

import com.datasqrl.engine.stream.flink.schema.FlinkTableSchemaGenerator;
import com.datasqrl.io.tables.TableSchemaVisitor;
import com.datasqrl.schema.UniversalTableBuilder;
import com.datasqrl.schema.input.FlexibleDatasetSchema.TableField;
import com.datasqrl.schema.input.InputTableSchema;
import com.datasqrl.schema.input.JsonTableSchema;
import org.apache.flink.table.api.Schema;

public class Schema2FlinkSchemaVisitor implements
    TableSchemaVisitor<Schema, InputTableSchema> {

  @Override
  public Schema accept(TableField tableField, InputTableSchema context) {
    UniversalTableBuilder tblBuilder = getUniversalTableBuilder(context);

    return FlinkTableSchemaGenerator.INSTANCE.convertSchema(tblBuilder);
  }

  @Override
  public Schema accept(JsonTableSchema jsonTableSchema, InputTableSchema context) {
    return null;
  }
}
