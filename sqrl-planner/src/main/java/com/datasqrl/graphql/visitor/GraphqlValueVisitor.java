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
