package com.datasqrl.calcite.table;

import com.datasqrl.calcite.SqrlRelBuilder;
import lombok.AllArgsConstructor;
import lombok.Setter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.type.SqlReturnTypeInference;

@AllArgsConstructor
public class LazySqlReturnTypeInference implements
  SqlReturnTypeInference {

  @Setter
  RelDataType relDataType;
  @Override
  public RelDataType inferReturnType(SqlOperatorBinding sqlOperatorBinding) {
    return SqrlRelBuilder.shadow(relDataType);
  }

}