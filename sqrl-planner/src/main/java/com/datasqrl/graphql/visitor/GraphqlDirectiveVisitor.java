package com.datasqrl.graphql.visitor;

import graphql.language.Directive;

public interface GraphqlDirectiveVisitor<R, C> {

  public R visitDirective(Directive node, C context);
}
