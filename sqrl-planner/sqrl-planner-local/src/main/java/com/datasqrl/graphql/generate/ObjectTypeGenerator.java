/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.graphql.generate;

import static com.datasqrl.graphql.generate.SchemaGeneratorUtil.conformName;
import static com.datasqrl.graphql.generate.SchemaGeneratorUtil.getOutputType;
import static com.datasqrl.graphql.generate.SchemaGeneratorUtil.getTypeName;
import static com.datasqrl.graphql.generate.SchemaGeneratorUtil.getTypeReference;
import static com.datasqrl.graphql.generate.SchemaGeneratorUtil.wrap;

import com.datasqrl.schema.Column;
import com.datasqrl.schema.FieldVisitor;
import com.datasqrl.schema.Relationship;
import com.datasqrl.schema.SQRLTable;
import com.datasqrl.schema.SQRLTable.SqrlTableVisitor;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLType;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.calcite.jdbc.CalciteSchemaVisitor;
import org.apache.calcite.jdbc.SqrlSchema;

/**
 * Create the object types
 */
public class ObjectTypeGenerator implements
    CalciteSchemaVisitor<Set<GraphQLType>, SchemaGeneratorContext>,
    SqrlTableVisitor<GraphQLType, SchemaGeneratorContext>,
    FieldVisitor<GraphQLFieldDefinition, SchemaGeneratorContext> {

  @Override
  public Set<GraphQLType> visit(SqrlSchema schema, SchemaGeneratorContext context) {
    return schema.getAllTables().stream()
        .filter(SchemaGeneratorUtil::isAccessible)
        .map(t -> t.accept(this, context))
        .collect(Collectors.toSet());
  }

  @Override
  public GraphQLType visit(SQRLTable table, SchemaGeneratorContext context) {
    return GraphQLObjectType.newObject()
        .name(getTypeName(table, context.getNames()))
        .fields(table.getFields().getAccessibleFields()
            .stream()
            .filter(SchemaGeneratorUtil::isAccessible)
            .map(f -> f.accept(this, context))
            .collect(Collectors.toList()))
        .build();
  }

  @Override
  public GraphQLFieldDefinition visit(Column column, SchemaGeneratorContext context) {
    return GraphQLFieldDefinition.newFieldDefinition()
        .name(column.getName().getDisplay())
        .type(wrap(getOutputType(column.getType()), column.getType()))
        .build();
  }

  @Override
  public GraphQLFieldDefinition visit(Relationship field, SchemaGeneratorContext context) {
    return GraphQLFieldDefinition.newFieldDefinition()
        .name(conformName(field.getName().getDisplay()))
        .type(wrap(getTypeReference(field.getToTable(), context.getNames()), field.getMultiplicity()))
        .arguments(field.accept(new ArgumentGenerator(), context))
        .build();
  }
}
