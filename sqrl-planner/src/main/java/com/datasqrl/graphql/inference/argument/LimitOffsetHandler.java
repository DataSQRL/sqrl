/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.graphql.inference.argument;

import com.datasqrl.graphql.inference.ArgumentSet;
import com.datasqrl.graphql.server.Model.Argument;
import com.datasqrl.graphql.server.Model.VariableArgument;
import graphql.Scalars;
import graphql.language.NonNullType;
import graphql.language.Type;
import graphql.language.TypeName;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

public class LimitOffsetHandler implements ArgumentHandler {

  @Override
  public Set<ArgumentSet> accept(ArgumentHandlerContextV1 context) {
    Set<ArgumentSet> set = new HashSet<>(context.getArgumentSet());
    for (ArgumentSet args : context.getArgumentSet()) {
      //No-op rel node, query must be constructed at query time
      Set<Argument> newArgs = new LinkedHashSet<>(args.getArgumentHandlers());
      newArgs.add(VariableArgument.builder().path(context.getArg().getName()).build());
      ArgumentSet relAndArg = new ArgumentSet(args.getRelNode(), newArgs,
          args.getArgumentParameters(), true);

      set.add(relAndArg);
    }

    return set;
  }

  @Override
  public boolean canHandle(ArgumentHandlerContextV1 context) {
    //must be int or not null int
    Type<?> type = context.getArg().getType();
    if (type instanceof NonNullType) {
      NonNullType nonNull = (NonNullType) type;
      type = nonNull.getType();
    }

    if (!(type instanceof TypeName) ||
        !((TypeName) type).getName().equalsIgnoreCase(Scalars.GraphQLInt.getName())) {
      return false;
    }

    return (context.getArg().getName().equalsIgnoreCase("limit") ||
        context.getArg().getName().equalsIgnoreCase("offset"));
  }
}