/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.queries;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.graphql.server.Model.Argument;
import com.datasqrl.graphql.server.Model.JdbcParameterHandler;
import java.util.ArrayList;
import java.util.List;
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
  NamePath namePath;
  RelNode relNode;
  List<JdbcParameterHandler> parameters;
  List<Argument> graphqlArguments;
  boolean isLimitOffset;

}
