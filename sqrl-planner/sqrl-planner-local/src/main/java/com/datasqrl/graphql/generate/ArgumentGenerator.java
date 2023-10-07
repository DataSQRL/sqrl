/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.graphql.generate;

import static com.datasqrl.graphql.generate.SchemaGeneratorUtil.getInputType;

import com.datasqrl.function.SqrlFunctionParameter;
import com.datasqrl.graphql.inference.SqrlSchema2.*;
import com.datasqrl.graphql.inference.SqrlSchema2.SQRLTable;
import com.datasqrl.graphql.inference.SqrlSchema2.SQRLTable.SqrlTableVisitor;
import com.datasqrl.schema.Multiplicity;
import com.datasqrl.schema.Relationship.JoinType;
import graphql.schema.GraphQLArgument;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.schema.FunctionParameter;

public class ArgumentGenerator implements
    FieldVisitor<List<GraphQLArgument>, SchemaGeneratorContext>,
    SqrlTableVisitor<List<GraphQLArgument>, SchemaGeneratorContext> {

  @Override
  public List<GraphQLArgument> visit(Column column, SchemaGeneratorContext context) {
    throw new RuntimeException("Columns have no arguments");
  }

  @Override
  public List<GraphQLArgument> visit(Relationship field, SchemaGeneratorContext context) {
    if (!allowedArguments(field)) {
      return List.of();
    }

    List<FunctionParameter> parameters = field.getParameters().stream()
        .filter(f->!((SqrlFunctionParameter)f).isInternal())
        .collect(Collectors.toList());
    if (parameters.isEmpty()) {
      return field.getToTable().accept(this, context);
    } else {
      return parameters.stream()
          .filter(p->!((SqrlFunctionParameter)p).isInternal())
          .map(parameter -> GraphQLArgument.newArgument()
              .name(parameter.getName())
              .type(getInputType(parameter.getType(null)))
              .build()).collect(Collectors.toList());
    }
  }

  @Override
  public List<GraphQLArgument> visit(SQRLTable table, SchemaGeneratorContext context) {
    return table.getColumns(true)
        .stream()
        .map(f -> GraphQLArgument.newArgument()
            .name(f.getName().getDisplay())
            .type(getInputType(f.getType()))
            .build())
        .limit(8)
        .collect(Collectors.toList());
  }

  private boolean allowedArguments(Relationship field) {
    //No arguments for to-one rels or parent fields
    return field.getMultiplicity().equals(Multiplicity.MANY) &&
        !field.getJoinType().equals(JoinType.PARENT);
  }
}