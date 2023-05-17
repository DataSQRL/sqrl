package com.datasqrl.graphql.visitor;

import graphql.language.Argument;

public interface GraphqlArgumentVisitor<R, C> {

  public R visitArgument(Argument node, C context);
}
