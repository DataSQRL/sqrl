package com.datasqrl.graphql.visitor;

import graphql.language.VariableDefinition;

public interface GraphqlVariableDefinitionVisitor<R, C> {

  public R visitVariableDefinition(
      VariableDefinition node, C context);
}
