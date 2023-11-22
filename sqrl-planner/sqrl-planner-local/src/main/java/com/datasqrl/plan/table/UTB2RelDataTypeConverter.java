package com.datasqrl.plan.table;

import com.datasqrl.calcite.type.TypeFactory;
import com.datasqrl.schema.UniversalTable;
import com.datasqrl.util.CalciteUtil;
import com.datasqrl.util.RelDataTypeBuilder;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import lombok.AllArgsConstructor;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.commons.lang3.tuple.Pair;

@AllArgsConstructor
public class UTB2RelDataTypeConverter implements UniversalTable.SchemaConverter<RelDataType> {


  @Override
  public RelDataType convertSchema(UniversalTable tblBuilder) {
    return tblBuilder.getType();
  }
}
