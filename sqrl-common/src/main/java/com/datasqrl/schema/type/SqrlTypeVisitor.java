/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.schema.type;


import com.datasqrl.schema.type.basic.*;

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

  default R visitDateTimeType(DateTimeType type, C context) {
    return visitBasicType(type, context);
  }

  default R visitFloatType(FloatType type, C context) {
    return visitBasicType(type, context);
  }

  default R visitIntegerType(IntegerType type, C context) {
    return visitBasicType(type, context);
  }

  default R visitStringType(StringType type, C context) {
    return visitBasicType(type, context);
  }

  default R visitUuidType(UuidType type, C context) {
    return visitBasicType(type, context);
  }

  default R visitIntervalType(IntervalType type, C context) {
    return visitBasicType(type, context);
  }

  default R visitArrayType(ArrayType type, C context) {
    return visitType(type, context);
  }

}
