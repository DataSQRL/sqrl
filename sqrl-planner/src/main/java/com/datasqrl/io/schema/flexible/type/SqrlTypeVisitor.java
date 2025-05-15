/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
