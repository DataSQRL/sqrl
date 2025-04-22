/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.io.schema.flexible.type.basic;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.schema.flexible.type.SqrlTypeVisitor;

public class ObjectType extends AbstractBasicType<Object> {

  public static final ObjectType INSTANCE = new ObjectType();

  @Override
  public List<String> getNames() {
    return List.of("OBJECT");
  }

  @Override
  public TypeConversion<Object> conversion() {
    return new Conversion();
  }

  public static class Conversion implements TypeConversion<Object> {

    public Conversion() {
    }

    @Override
    public Set<Class> getJavaTypes() {
      return Collections.singleton(Object.class);
    }

    @Override
	public Object convert(Object o) {
      return o;
    }

    @Override
    public Optional<Integer> getTypeDistance(BasicType fromType) {
      return Optional.of(100);
    }

    @Override
	public Optional<Object> parseDetected(Object original, ErrorCollector errors) {
      return Optional.of(original);
    }

  }

  @Override
public <R, C> R accept(SqrlTypeVisitor<R, C> visitor, C context) {
    return visitor.visitObjectType(this, context);
  }
}
