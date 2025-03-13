/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.queries;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.graphql.server.RootGraphqlModel.Argument;
import com.datasqrl.graphql.server.RootGraphqlModel.JdbcParameterHandler;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import lombok.EqualsAndHashCode;
import lombok.EqualsAndHashCode.Include;
import lombok.ToString;
import lombok.Value;
import org.apache.calcite.rel.RelNode;

@Value
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
@ToString
public class APIQuery implements IdentifiedQuery {

  @Include
  String nameId;
  // Path from root
  NamePath namePath;
  // Query
  RelNode relNode;
  // Where parameters come from (where parameters come from at runtime)
  List<JdbcParameterHandler> parameters;
  // How arguments are matched (matching the arguments in the signature of the functions)
  List<Argument> graphqlArguments;
  // Has a limit/offset
  boolean isLimitOffset;


  @Override
  public Optional<Name> getViewName() {
    if (!getParameters().isEmpty()) return Optional.empty();
    return Optional.of(namePath.getLast());
  }

}
