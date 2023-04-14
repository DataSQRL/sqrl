package com.datasqrl.engine.stream.flink.sql;

import com.datasqrl.schema.converters.FlinkTypeInfoSchemaGenerator;
import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;

public class RelNodeToTypeInformationTransformer {

  public TypeInformation transform(RelNode relNode) {
    List<RelDataTypeField> fields = relNode.getRowType().getFieldList();
    TypeInformation[] fieldTypes = new TypeInformation[fields.size()];
    FlinkTypeInfoSchemaGenerator converter = new FlinkTypeInfoSchemaGenerator();

    for (int i = 0; i < fields.size(); i++) {
      RelDataTypeField field = fields.get(i);
      fieldTypes[i] =  converter.convertBasic(field.getType());
    }

    return new RowTypeInfo(fieldTypes, relNode.getRowType().getFieldNames().toArray(new String[0]));
  }
}
