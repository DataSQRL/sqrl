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
package com.datasqrl.graphql.visitor;

import graphql.language.ArrayValue;
import graphql.language.BooleanValue;
import graphql.language.EnumValue;
import graphql.language.FloatValue;
import graphql.language.IntValue;
import graphql.language.NullValue;
import graphql.language.ObjectValue;
import graphql.language.StringValue;
import graphql.language.VariableReference;

public interface GraphqlValueVisitor<R, C> {
  public R visitArrayValue(ArrayValue node, C context);

  public R visitBooleanValue(BooleanValue node, C context);

  public R visitEnumValue(EnumValue node, C context);

  public R visitFloatValue(FloatValue node, C context);

  public R visitIntValue(IntValue node, C context);

  public R visitNullValue(NullValue node, C context);

  public R visitObjectValue(ObjectValue node, C context);

  public R visitStringValue(StringValue node, C context);

  public R visitVariableReference(VariableReference node, C context);
}
