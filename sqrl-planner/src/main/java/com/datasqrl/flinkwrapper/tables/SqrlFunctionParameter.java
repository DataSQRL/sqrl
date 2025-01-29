package com.datasqrl.flinkwrapper.tables;

import com.datasqrl.function.SqrlFunctionParameter.ParameterName;
import java.util.Optional;
import lombok.Getter;
import lombok.Value;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlNode;

/**
 * A parameter of {@link SqrlTableFunction}
 */
@Value
public class SqrlFunctionParameter implements FunctionParameter {

  String name; //the properly resolved name of the argument
  int ordinal; //the ordinal within the query
  RelDataType relDataType;  //this is the type of the argument
  boolean isParentField; //if true, this is a column on the "this" table, else a user provided argument

  @Override
  public RelDataType getType(RelDataTypeFactory relDataTypeFactory) {
    return relDataType;
  }

  @Override
  public boolean isOptional() {
    return false;
  }
}
