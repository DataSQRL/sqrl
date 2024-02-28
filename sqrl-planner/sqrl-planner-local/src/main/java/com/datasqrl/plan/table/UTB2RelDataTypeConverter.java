package com.datasqrl.plan.table;

import com.datasqrl.schema.UniversalTable;
import com.datasqrl.schema.UniversalTable.SchemaConverterUTB;
import lombok.AllArgsConstructor;
import org.apache.calcite.rel.type.RelDataType;

@AllArgsConstructor
public class UTB2RelDataTypeConverter implements SchemaConverterUTB<RelDataType> {


  @Override
  public RelDataType convertSchema(UniversalTable tblBuilder) {
    return tblBuilder.getType();
  }
}
