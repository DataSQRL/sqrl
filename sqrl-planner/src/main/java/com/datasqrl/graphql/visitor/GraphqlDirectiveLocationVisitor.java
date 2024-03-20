package com.datasqrl.graphql.visitor;

import graphql.language.DirectiveLocation;

public interface GraphqlDirectiveLocationVisitor<R, C> {

  public R visitDirectiveLocation(DirectiveLocation node, C context);
}
