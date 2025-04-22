/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.io.schema.flexible.type;


import com.datasqrl.io.schema.flexible.type.basic.AbstractBasicType;
import com.datasqrl.io.schema.flexible.type.basic.BigIntType;
import com.datasqrl.io.schema.flexible.type.basic.BooleanType;
import com.datasqrl.io.schema.flexible.type.basic.DoubleType;
import com.datasqrl.io.schema.flexible.type.basic.IntervalType;
import com.datasqrl.io.schema.flexible.type.basic.ObjectType;
import com.datasqrl.io.schema.flexible.type.basic.StringType;
import com.datasqrl.io.schema.flexible.type.basic.TimestampType;

public interface SqrlTypeVisitor<R, C> {

  default R visitType(Type type, C context) {
    return null;
  }

  default <J> R visitBasicType(AbstractBasicType<J> type, C context) {
    return visitType(type, context);
  }

  default R visitBooleanType(BooleanType type, C context) {
    return visitBasicType(type, context);
  }

  default R visitTimestampType(TimestampType type, C context) {
    return visitBasicType(type, context);
  }

  default R visitDoubleType(DoubleType type, C context) {
    return visitBasicType(type, context);
  }

  default R visitBigIntType(BigIntType type, C context) {
    return visitBasicType(type, context);
  }

  default R visitStringType(StringType type, C context) {
    return visitBasicType(type, context);
  }

  default R visitIntervalType(IntervalType type, C context) {
    return visitBasicType(type, context);
  }

  default R visitArrayType(ArrayType type, C context) {
    return visitType(type, context);
  }

  default R visitObjectType(ObjectType type, C context) {
    return visitType(type, context);
  }
}
