/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.graphql.inference;

import com.datasqrl.graphql.server.Model.Argument;
import com.datasqrl.graphql.server.Model.ArgumentParameter;
import java.util.List;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.apache.calcite.rel.RelNode;

@AllArgsConstructor
@Getter
public class ArgumentSet {

  RelNode relNode;
  Set<Argument> argumentHandlers;
  List<ArgumentParameter> argumentParameters;
  @Setter
  boolean limitOffsetFlag;
}