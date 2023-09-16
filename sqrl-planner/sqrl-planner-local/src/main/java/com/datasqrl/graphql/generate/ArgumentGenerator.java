/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.graphql.generate;

import static com.datasqrl.graphql.generate.SchemaGeneratorUtil.getInputType;
import static com.ibm.icu.text.PluralRules.Operand.f;

import com.datasqrl.function.SqrlFunctionParameter;
import com.datasqrl.schema.Column;
import com.datasqrl.schema.FieldVisitor;
import com.datasqrl.schema.Multiplicity;
import com.datasqrl.schema.Relationship;
import com.datasqrl.schema.Relationship.JoinType;
import com.datasqrl.schema.SQRLTable;
import com.datasqrl.schema.SQRLTable.SqrlTableVisitor;
import graphql.schema.GraphQLArgument;
import java.util.ArrayList;
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

    List<FunctionParameter> parameters = field.getParameters();
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
    return table.getFields().getAccessibleFields()
        .stream().filter(SchemaGeneratorUtil::isAccessible)
        .filter(f -> f instanceof Column)
        .map(f -> GraphQLArgument.newArgument()
            .name(f.getName().getDisplay())
            .type(getInputType(((Column) f).getType()))
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