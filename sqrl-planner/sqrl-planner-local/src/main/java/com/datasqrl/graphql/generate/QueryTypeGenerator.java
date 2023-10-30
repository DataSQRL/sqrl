/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.graphql.generate;

import static com.datasqrl.graphql.generate.ObjectTypeGenerator.logIfInvalid;
import static com.datasqrl.graphql.generate.SchemaGenerator.isValidGraphQLName;
import static com.datasqrl.graphql.generate.SchemaGeneratorUtil.getTypeReference;
import static com.datasqrl.graphql.generate.SchemaGeneratorUtil.wrap;

import com.datasqrl.graphql.inference.SqrlSchemaForInference;
import com.datasqrl.graphql.inference.SqrlSchemaForInference.CalciteSchemaVisitor;
import com.datasqrl.graphql.inference.SqrlSchemaForInference.SQRLTable;
import com.datasqrl.graphql.inference.SqrlSchemaForInference.SQRLTable.SqrlTableVisitor;
import com.datasqrl.schema.Multiplicity;
import graphql.schema.GraphQLFieldDefinition;
import java.util.List;

/**
 * Generates the GraphQL Query type.
 */
public class QueryTypeGenerator implements
    SqrlTableVisitor<Void, SchemaGeneratorContext>,
    CalciteSchemaVisitor<Void, SchemaGeneratorContext> {

  private final List<GraphQLFieldDefinition> queryFields;
  private final boolean addArguments;

  public QueryTypeGenerator(List<GraphQLFieldDefinition> queryFields, boolean addArguments) {
    this.queryFields = queryFields;
    this.addArguments = addArguments;
  }

  @Override
  public Void visit(SqrlSchemaForInference schema, SchemaGeneratorContext context) {
    schema.getRootTables().stream()
        .filter(SchemaGeneratorUtil::isAccessible)
        .forEach(t -> t.accept(this, context));

    return null;
  }

  @Override
  public Void visit(SQRLTable table, SchemaGeneratorContext context) {
    if (!logIfInvalid(isValidGraphQLName(table.getName()), context.sqrlTable)) {
      return null;
    }

    GraphQLFieldDefinition field = GraphQLFieldDefinition.newFieldDefinition()
        .name(table.getName())
        .type(wrap(getTypeReference(table, context.getNames()), Multiplicity.MANY))
        .arguments(table.accept(new ArgumentGenerator(addArguments), context))
        .build();
    queryFields.add(field);

    return null;
  }
}