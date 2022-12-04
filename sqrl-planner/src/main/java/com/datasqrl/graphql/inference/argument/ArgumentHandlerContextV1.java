/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.graphql.inference.argument;

import com.datasqrl.graphql.inference.ArgumentSet;
import com.datasqrl.graphql.server.Model.PgParameterHandler;
import com.datasqrl.schema.SQRLTable;
import graphql.language.InputValueDefinition;
import java.util.List;
import java.util.Set;
import lombok.Value;
import org.apache.calcite.tools.RelBuilder;

@Value
public class ArgumentHandlerContextV1 {

  InputValueDefinition arg;
  Set<ArgumentSet> argumentSet;
  SQRLTable table;
  RelBuilder relBuilder;
  List<PgParameterHandler> sourceHandlers;
}