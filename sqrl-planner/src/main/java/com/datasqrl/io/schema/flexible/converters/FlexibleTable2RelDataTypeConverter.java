/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.io.schema.flexible.converters;

import java.util.ArrayDeque;
import java.util.Deque;

import org.apache.calcite.rel.type.RelDataType;

import com.datasqrl.calcite.type.TypeFactory;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.io.schema.flexible.FlexibleTableConverter.Visitor;
import com.datasqrl.io.schema.flexible.type.Type;
import com.datasqrl.util.CalciteUtil;
import com.datasqrl.util.RelDataTypeBuilder;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
public class FlexibleTable2RelDataTypeConverter implements
    Visitor<RelDataType> {

  private final SqrlTypeRelDataTypeConverter typeConverter;
  private final Deque<RelDataTypeBuilder> stack = new ArrayDeque<>();
  @Getter
  private final TypeFactory typeFactory;

  public FlexibleTable2RelDataTypeConverter() {
    this(TypeFactory.getTypeFactory());
  }

  public FlexibleTable2RelDataTypeConverter(TypeFactory typeFactory) {
    this.typeConverter = new SqrlTypeRelDataTypeConverter(typeFactory);
    this.typeFactory = typeFactory;

  }

  @Override
  public void beginTable(Name name, NamePath namePath, boolean isNested, boolean isSingleton) {
    var builder = CalciteUtil.getRelTypeBuilder(typeFactory);
    if (isNested && !isSingleton) {
      //TODO: For flexible schema we add nested array indexes since ordinals are not yet supported in unnesting in Flink
//      builder.add(ReservedName.ARRAY_IDX, TypeFactory.makeIntegerType(typeFactory, false));
    }
    stack.addFirst(builder);
  }

  @Override
  public RelDataType endTable(Name name, NamePath namePath, boolean isNested,
      boolean isSingleton) {
    var type = stack.removeFirst().build();
    if (!isSingleton) {
      type = typeFactory.wrapInArray(type);
      type = typeFactory.createTypeWithNullability(type, false);
    }
    return type;
  }

  @Override
  public void addField(Name name, Type type, boolean nullable) {
    var tblBuilder = stack.getFirst();
    tblBuilder.add(name, typeFactory.createTypeWithNullability(type.accept(typeConverter, null), nullable));
  }

  @Override
  public void addField(Name name, RelDataType nestedTable, boolean nullable,
      boolean isSingleton) {
    var tblBuilder = stack.getFirst();
    tblBuilder.add(name, typeFactory.createTypeWithNullability(nestedTable, nullable));
  }

}
