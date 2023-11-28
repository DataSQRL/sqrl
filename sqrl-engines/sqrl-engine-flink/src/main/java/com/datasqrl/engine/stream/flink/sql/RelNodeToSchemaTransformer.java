package com.datasqrl.engine.stream.flink.sql;

import com.datasqrl.schema.converters.UniversalTable2FlinkSchema;
import com.datasqrl.serializer.SerializableSchema;
import com.datasqrl.serializer.SerializableSchema.WaterMarkType;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.commons.lang3.tuple.Pair;

public class RelNodeToSchemaTransformer {

  public SerializableSchema transform(RelNode relNode, int primaryKeyCount) {
    SerializableSchema.SerializableSchemaBuilder builder = SerializableSchema.builder();
    UniversalTable2FlinkSchema converter = new UniversalTable2FlinkSchema();
    List<RelDataTypeField> fields = relNode.getRowType().getFieldList();
    // TODO: a primary key column should never be null. We should be able to replace this by UniversalTable2FlinkSchema#convert
    for (int i = 0; i < fields.size(); i++) {
      RelDataTypeField field = fields.get(i);
      boolean isNotNull = !field.getType().isNullable() || i < primaryKeyCount;
      builder.column(Pair.of(field.getName(), converter.nullable(
          converter.convertPrimitive(field.getType()), !isNotNull)));
    }

    if (primaryKeyCount != 0) {
      builder.primaryKey(createPrimaryKeyList(relNode, primaryKeyCount));
    }

    builder.waterMarkType(WaterMarkType.NONE);

    return builder.build();
  }

  private List<String> createPrimaryKeyList(RelNode relNode, int primaryKeyCount) {
    return relNode.getRowType().getFieldList().subList(0, primaryKeyCount)
        .stream().map(RelDataTypeField::getName)
        .collect(Collectors.toList());
  }
}
