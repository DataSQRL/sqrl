package com.datasqrl.graphql.visitor;

import graphql.language.Field;
import graphql.language.FragmentSpread;
import graphql.language.InlineFragment;

public interface GraphqlSelectionVisitor<R, C> {
  public R visitField(Field node, C context);

  public R visitFragmentSpread(FragmentSpread node, C context);

  public R visitInlineFragment(InlineFragment node, C context);
}
