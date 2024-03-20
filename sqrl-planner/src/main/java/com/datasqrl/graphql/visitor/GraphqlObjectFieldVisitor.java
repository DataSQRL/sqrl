package com.datasqrl.graphql.visitor;

import graphql.language.ObjectField;

public interface GraphqlObjectFieldVisitor<R, C> {

  public R visitObjectField(ObjectField node, C context);
}
