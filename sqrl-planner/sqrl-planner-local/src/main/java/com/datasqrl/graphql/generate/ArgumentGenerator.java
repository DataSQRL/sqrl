/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.graphql.generate;

import static com.datasqrl.graphql.generate.ObjectTypeGenerator.logIfInvalid;
import static com.datasqrl.graphql.generate.SchemaGenerator.isValidGraphQLName;
import static com.datasqrl.graphql.generate.SchemaGeneratorUtil.getInputType;
import static com.datasqrl.graphql.jdbc.SchemaConstants.LIMIT;
import static com.datasqrl.graphql.jdbc.SchemaConstants.OFFSET;
import static graphql.schema.GraphQLNonNull.nonNull;

import com.datasqrl.function.SqrlFunctionParameter;
import com.datasqrl.graphql.inference.SqrlSchemaForInference.Column;
import com.datasqrl.graphql.inference.SqrlSchemaForInference.FieldVisitor;
import com.datasqrl.graphql.inference.SqrlSchemaForInference.Relationship;
import com.datasqrl.graphql.inference.SqrlSchemaForInference.SQRLTable;
import com.datasqrl.graphql.inference.SqrlSchemaForInference.SQRLTable.SqrlTableVisitor;
import com.datasqrl.schema.Multiplicity;
import com.datasqrl.schema.Relationship.JoinType;
import graphql.Scalars;
import graphql.language.IntValue;
import graphql.schema.GraphQLArgument;
import graphql.schema.GraphQLInputType;
import java.util.List;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.commons.collections.ListUtils;

@AllArgsConstructor
public class ArgumentGenerator implements
    FieldVisitor<List<GraphQLArgument>, SchemaGeneratorContext>,
    SqrlTableVisitor<List<GraphQLArgument>, SchemaGeneratorContext> {

  private final boolean addArguments;

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
    if (addArguments && parameters.isEmpty() && field.getJoinType() == JoinType.JOIN) {
      List<GraphQLArgument> limitOffset = generateLimitOffset();
      return limitOffset;
    } else if (addArguments && parameters.isEmpty()) {
      List<GraphQLArgument> premuted = generatePermuted(field.getToTable());
      List<GraphQLArgument> limitOffset = generateLimitOffset();

      return ListUtils.union(premuted, limitOffset);
    } else {
      return parameters.stream()
          .filter(p->!((SqrlFunctionParameter)p).isInternal())
          .filter(p->getInputType(p.getType(null)).isPresent())
          .map(parameter -> GraphQLArgument.newArgument()
              .name(((SqrlFunctionParameter)parameter).getVariableName())
              .type(nonNull(getInputType(parameter.getType(null)).get()))
              .build()).collect(Collectors.toList());
    }
  }

  @Override
  public List<GraphQLArgument> visit(SQRLTable table, SchemaGeneratorContext context) {
    List<FunctionParameter> params = table.getTableMacro().getParameters().stream()
        .filter(f -> !((SqrlFunctionParameter) f).isInternal())
        .collect(Collectors.toList());

    if (!params.isEmpty()) {
      return params.stream()
          .map(a->(SqrlFunctionParameter)a)
          .filter(a -> getInputType(a.getRelDataType()).isPresent())
          .filter(a-> logIfInvalid(isValidGraphQLName(a.getVariableName()), table, a))
          .map(this::createArgument)
          .collect(Collectors.toList());
    } else if (addArguments) {
      List<GraphQLArgument> premuted = generatePermuted(table);
      List<GraphQLArgument> limitOffset = generateLimitOffset();
      return ListUtils.union(premuted, limitOffset);
    }

    return List.of();
  }

  private List<GraphQLArgument> generateLimitOffset() {

    //add limit / offset
    GraphQLArgument limit = GraphQLArgument.newArgument()
        .name(LIMIT)
        .type(Scalars.GraphQLInt)
        .defaultValueLiteral(IntValue.of(10))
        .build();

    GraphQLArgument offset = GraphQLArgument.newArgument()
        .name(OFFSET)
        .type(Scalars.GraphQLInt)
        .defaultValueLiteral(IntValue.of(0))
        .build();
    return List.of(limit, offset);
  }

  private List<GraphQLArgument> generatePermuted(SQRLTable table) {
    return table.getPrimaryKeys()
        .stream()
        .filter(f -> getInputType(f.getType()).isPresent())
        .filter(f -> logIfInvalid(isValidGraphQLName(f.getName().getDisplay()), table, f))
        .map(f -> GraphQLArgument.newArgument()
            .name(f.getName().getDisplay())
            .type(getInputType(f.getType()).get())
            .build())
        .collect(Collectors.toList());
  }

  private GraphQLArgument createArgument(FunctionParameter parameter) {
    SqrlFunctionParameter sqrlFunctionParameter = (SqrlFunctionParameter)parameter;
    //todo check if type is real
    GraphQLInputType argType = getInputType(sqrlFunctionParameter.getRelDataType()).get();
    return GraphQLArgument.newArgument()
        .name(sqrlFunctionParameter.getVariableName())
        .type(nonNull(argType))
        .build();
  }
  private boolean allowedArguments(Relationship field) {
    //No arguments for to-one rels or parent fields
    return field.getMultiplicity().equals(Multiplicity.MANY) &&
        !field.getJoinType().equals(JoinType.PARENT);
  }
}