package com.datasqrl.graphql.visitor;

import graphql.language.InputValueDefinition;

public interface GraphqlInputValueDefinitionVisitor<R, C> {

  public R visitInputValueDefinition(InputValueDefinition node, C context);
}
