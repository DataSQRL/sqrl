package com.datasqrl.graphql.visitor;

import graphql.language.SelectionSet;

public interface GraphqlSelectionSetVisitor<R, C> {

  public R visitSelectionSet(SelectionSet node, C context);
}
