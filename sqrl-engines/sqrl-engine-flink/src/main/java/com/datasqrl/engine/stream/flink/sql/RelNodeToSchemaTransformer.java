package com.datasqrl.engine.stream.flink.sql;

import com.datasqrl.schema.converters.UniversalTable2FlinkSchema;
import com.datasqrl.serializer.SerializableSchema;
import com.datasqrl.serializer.SerializableSchema.WaterMarkType;
import com.datasqrl.util.ArrayUtil;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.commons.lang3.tuple.Pair;

public class RelNodeToSchemaTransformer {

  public SerializableSchema transform(RelNode relNode, int[] primaryKeys) {
    SerializableSchema.SerializableSchemaBuilder builder = SerializableSchema.builder();
    UniversalTable2FlinkSchema converter = new UniversalTable2FlinkSchema();
    List<RelDataTypeField> fields = relNode.getRowType().getFieldList();
    // TODO: a primary key column should never be null. We should be able to replace this by UniversalTable2FlinkSchema#convert
    for (int i = 0; i < fields.size(); i++) {
      RelDataTypeField field = fields.get(i);
      boolean isNotNull = !field.getType().isNullable() || (primaryKeys!=null && ArrayUtil.contains(primaryKeys, i));
      builder.column(Pair.of(field.getName(), converter.nullable(
          converter.convertPrimitive(field.getType()), !isNotNull)));
    }

    if (primaryKeys != null) {
      builder.primaryKey(IntStream.of(primaryKeys).mapToObj(fields::get).map(RelDataTypeField::getName)
          .collect(Collectors.toList()));
    }

    builder.waterMarkType(WaterMarkType.NONE);

    return builder.build();
  }
}
