package com.datasqrl.planner.tables;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.FunctionParameter;

import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.Value;

/**
 * A parameter of {@link SqrlTableFunction}
 */
@Value
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
@ToString(onlyExplicitlyIncluded = true)
public class SqrlFunctionParameter implements FunctionParameter {

  @EqualsAndHashCode.Include @ToString.Include
  String name; //the properly resolved name of the argument
  int ordinal; //the index within the list of query arguments
  @EqualsAndHashCode.Include @ToString.Include
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
