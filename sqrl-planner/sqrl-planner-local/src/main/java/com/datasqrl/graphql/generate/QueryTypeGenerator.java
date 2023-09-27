/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.graphql.generate;

import static com.datasqrl.graphql.generate.SchemaGeneratorUtil.conformName;
import static com.datasqrl.graphql.generate.SchemaGeneratorUtil.getTypeReference;
import static com.datasqrl.graphql.generate.SchemaGeneratorUtil.wrap;

import com.datasqrl.graphql.inference.SqrlSchemaForInference;
import com.datasqrl.graphql.inference.SqrlSchemaForInference.CalciteSchemaVisitor;
import com.datasqrl.graphql.inference.SqrlSchemaForInference.SQRLTable;
import com.datasqrl.graphql.inference.SqrlSchemaForInference.SQRLTable.SqrlTableVisitor;
import com.datasqrl.schema.Multiplicity;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLObjectType;
import java.util.stream.Collectors;

/**
 * Generates the GraphQL Query type.
 */
public class QueryTypeGenerator implements
    SqrlTableVisitor<GraphQLFieldDefinition, SchemaGeneratorContext>,
    CalciteSchemaVisitor<GraphQLObjectType, SchemaGeneratorContext> {

  @Override
  public GraphQLObjectType visit(SqrlSchemaForInference schema, SchemaGeneratorContext context) {
    return GraphQLObjectType.newObject()
        .name("Query")
        .fields(schema.getRootTables().stream()
            .filter(SchemaGeneratorUtil::isAccessible)
            .map(t -> t.accept(this, context))
            .collect(Collectors.toList()))
        .build();
  }

  @Override
  public GraphQLFieldDefinition visit(SQRLTable table, SchemaGeneratorContext context) {
    return GraphQLFieldDefinition.newFieldDefinition()
        .name(conformName(table.getName()))
        .type(wrap(getTypeReference(table, context.getNames()), Multiplicity.MANY))
        .arguments(table.accept(new ArgumentGenerator(), context))
        .build();
  }
}