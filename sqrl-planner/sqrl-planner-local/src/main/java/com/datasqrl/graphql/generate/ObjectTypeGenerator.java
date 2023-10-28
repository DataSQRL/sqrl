/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.graphql.generate;

import static com.datasqrl.canonicalizer.Name.SYSTEM_HIDDEN_PREFIX;
import static com.datasqrl.graphql.generate.SchemaGeneratorUtil.conformName;
import static com.datasqrl.graphql.generate.SchemaGeneratorUtil.getOutputType;
import static com.datasqrl.graphql.generate.SchemaGeneratorUtil.getTypeName;
import static com.datasqrl.graphql.generate.SchemaGeneratorUtil.getTypeReference;
import static com.datasqrl.graphql.generate.SchemaGeneratorUtil.wrap;

import com.datasqrl.graphql.inference.SqrlSchemaForInference;
import com.datasqrl.graphql.inference.SqrlSchemaForInference.CalciteSchemaVisitor;
import com.datasqrl.graphql.inference.SqrlSchemaForInference.Column;
import com.datasqrl.graphql.inference.SqrlSchemaForInference.FieldVisitor;
import com.datasqrl.graphql.inference.SqrlSchemaForInference.Relationship;
import com.datasqrl.graphql.inference.SqrlSchemaForInference.SQRLTable;
import com.datasqrl.graphql.inference.SqrlSchemaForInference.SQRLTable.SqrlTableVisitor;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLType;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Create the object types
 */
public class ObjectTypeGenerator implements
    CalciteSchemaVisitor<Set<GraphQLType>, SchemaGeneratorContext>,
    SqrlTableVisitor<GraphQLType, SchemaGeneratorContext>,
    FieldVisitor<GraphQLFieldDefinition, SchemaGeneratorContext> {

  @Override
  public Set<GraphQLType> visit(SqrlSchemaForInference schema, SchemaGeneratorContext context) {
    return schema.getAllTables().stream()
        .filter(SchemaGeneratorUtil::isAccessible)
        .map(t -> t.accept(this, context))
        .collect(Collectors.toSet());
  }

  @Override
  public GraphQLType visit(SQRLTable table, SchemaGeneratorContext context) {
    List<GraphQLFieldDefinition> fields = table.getFields(true)
        .stream()
        .filter(f -> !(f instanceof Column) || getOutputType(((Column) f).getType()).isPresent())
        .map(f -> f.accept(this, context))
        .collect(Collectors.toList());

    //Include hidden fields in relation has none.
    if (fields.isEmpty()) {
      fields = table.getFields(false)
          .stream()
          .filter(f->!f.getName().getDisplay().startsWith(SYSTEM_HIDDEN_PREFIX))
          .filter(f -> !(f instanceof Column) || getOutputType(((Column) f).getType()).isPresent())
          .map(f -> f.accept(this, context))
          .collect(Collectors.toList());
    }

    return GraphQLObjectType.newObject()
        .name(getTypeName(table, context.getNames()))
        .fields(fields)
        .build();
  }

  @Override
  public GraphQLFieldDefinition visit(Column column, SchemaGeneratorContext context) {
    return GraphQLFieldDefinition.newFieldDefinition()
        .name(conformName(column.getName().getDisplay()))
        .type(wrap(getOutputType(column.getType()).get(), column.getType()))
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
