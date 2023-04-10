package com.datasqrl.engine.stream.flink.sql;

import com.datasqrl.engine.stream.flink.schema.UniversalTable2FlinkSchema;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.flink.table.api.Schema;

public class RelNodeToSchemaTransformer {

  public Schema transform(RelNode relNode, int primaryKeyCount) {
    Schema.Builder builder = Schema.newBuilder();
    UniversalTable2FlinkSchema converter = new UniversalTable2FlinkSchema();

    for (int i = 0; i < relNode.getRowType().getFieldCount(); i++) {
      RelDataTypeField field = relNode.getRowType().getFieldList().get(i);
      boolean isNotNull = !field.getType().isNullable() || i < primaryKeyCount;

      builder.column(field.getName(), converter.nullable(
          converter.convertBasic(field.getType()), !isNotNull));
    }

    if (primaryKeyCount != 0) {
      builder.primaryKey(createPrimaryKeyList(relNode, primaryKeyCount));
    }

    return builder.build();
  }

  private List<String> createPrimaryKeyList(RelNode relNode, int primaryKeyCount) {
    return relNode.getRowType().getFieldList().subList(0, primaryKeyCount)
        .stream().map(RelDataTypeField::getName)
        .collect(Collectors.toList());
  }
}
