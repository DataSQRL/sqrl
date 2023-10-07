/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.graphql.generate;

import com.datasqrl.graphql.inference.SqrlSchemaForInference;
import graphql.schema.GraphQLSchema;

/**
 * Creates a default graphql schema based on the SQRL schema
 */
public class SchemaGenerator {
  public GraphQLSchema generate(SqrlSchemaForInference schema) {
    SchemaGeneratorContext context = new SchemaGeneratorContext();
    GraphQLSchema.Builder builder = GraphQLSchema.newSchema();
    builder.query(schema.accept(new QueryTypeGenerator(), context));
    builder.additionalTypes(schema.accept(new ObjectTypeGenerator(), context));
    return builder.build();
  }
}