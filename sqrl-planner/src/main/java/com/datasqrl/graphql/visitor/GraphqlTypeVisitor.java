package com.datasqrl.graphql.visitor;

import graphql.language.ListType;
import graphql.language.NonNullType;
import graphql.language.TypeName;

public interface GraphqlTypeVisitor<R, C> {

  public R visitListType(ListType node, C context);

  public R visitNonNullType(NonNullType node, C context);

  public R visitTypeName(TypeName node, C context);
}
