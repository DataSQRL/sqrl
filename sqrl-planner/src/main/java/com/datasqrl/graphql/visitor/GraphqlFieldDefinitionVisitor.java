package com.datasqrl.graphql.visitor;

import graphql.language.FieldDefinition;

public interface GraphqlFieldDefinitionVisitor<R, C> {

  public R visitFieldDefinition(FieldDefinition node, C context);
}
