/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.queries;

import java.util.List;
import java.util.Optional;

import org.apache.calcite.rel.RelNode;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.graphql.server.RootGraphqlModel.Argument;
import com.datasqrl.graphql.server.RootGraphqlModel.JdbcParameterHandler;

import lombok.EqualsAndHashCode;
import lombok.EqualsAndHashCode.Include;
import lombok.ToString;
import lombok.Value;

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
  // Where parameters come from
  List<JdbcParameterHandler> parameters;
  // How arguments are matched
  List<Argument> graphqlArguments;
  // Has a limit/offset
  boolean isLimitOffset;


  @Override
  public Optional<Name> getViewName() {
    if (!getParameters().isEmpty()) {
		return Optional.empty();
	}
    return Optional.of(namePath.getLast());
  }

}
