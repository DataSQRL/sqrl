/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.graphql.generate;

import static com.datasqrl.graphql.generate.SchemaGenerator.isValidGraphQLName;
import static com.datasqrl.graphql.generate.SchemaGeneratorUtil.conformName;
import static com.datasqrl.graphql.generate.SchemaGeneratorUtil.getOutputType;
import static com.datasqrl.graphql.generate.SchemaGeneratorUtil.getTypeName;
import static com.datasqrl.graphql.generate.SchemaGeneratorUtil.getTypeReference;
import static com.datasqrl.graphql.generate.SchemaGeneratorUtil.wrap;

import com.datasqrl.graphql.inference.SqrlSchemaForInference;
import com.datasqrl.graphql.inference.SqrlSchemaForInference.CalciteSchemaVisitor;
import com.datasqrl.graphql.inference.SqrlSchemaForInference.Column;
import com.datasqrl.graphql.inference.SqrlSchemaForInference.Field;
import com.datasqrl.graphql.inference.SqrlSchemaForInference.FieldVisitor;
import com.datasqrl.graphql.inference.SqrlSchemaForInference.Relationship;
import com.datasqrl.graphql.inference.SqrlSchemaForInference.SQRLTable;
import com.datasqrl.graphql.inference.SqrlSchemaForInference.SQRLTable.SqrlTableVisitor;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLObjectType;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.schema.FunctionParameter;

/**
 * Create the object types
 */
@Slf4j
public class ObjectTypeGenerator implements
    CalciteSchemaVisitor<Void, SchemaGeneratorContext>,
    SqrlTableVisitor<Void, SchemaGeneratorContext>,
    FieldVisitor<Optional<GraphQLFieldDefinition>, SchemaGeneratorContext> {

  private final List<GraphQLObjectType> objectTypes;

  public ObjectTypeGenerator(List<GraphQLObjectType> objectTypes) {
    this.objectTypes = objectTypes;
  }

  @Override
  public Void visit(SqrlSchemaForInference schema, SchemaGeneratorContext context) {
     schema.getAllTables().stream()
        .filter(SchemaGeneratorUtil::isAccessible)
        .forEach(t -> t.accept(this, context));
    return null;
  }

  @Override
  public Void visit(SQRLTable table, SchemaGeneratorContext context) {
    List<GraphQLFieldDefinition> fields = table.getFields(true)
        .stream()
        .filter(f->logIfInvalid(isValidGraphQLName(f.getName().getDisplay()), table, f))
        .filter(f -> !(f instanceof Column) || getOutputType(((Column) f).getType()).isPresent())
        .map(f -> f.accept(this, context.setSqrlTable(table)))
        .filter(Optional::isPresent)
        .map(Optional::get)
        .collect(Collectors.toList());

    if (fields.isEmpty()) {
      return null;
    }

    GraphQLObjectType objectType = GraphQLObjectType.newObject()
        .name(conformName(getTypeName(table, context.getNames())))
        .fields(fields)
        .build();
    objectTypes.add(objectType);
    return null;
  }

  @Override
  public Optional<GraphQLFieldDefinition> visit(Column column, SchemaGeneratorContext context) {
    if (!logIfInvalid(isValidGraphQLName(column.getName().getDisplay()), context.sqrlTable, column)) {
      return Optional.empty();
    }

    return Optional.of(GraphQLFieldDefinition.newFieldDefinition()
        .name(column.getName().getDisplay())
        .type(wrap(getOutputType(column.getType()).get(), column.getType()))
        .build());
  }

  @Override
  public Optional<GraphQLFieldDefinition> visit(Relationship field, SchemaGeneratorContext context) {
    if (!logIfInvalid(isValidGraphQLName(field.getName().getDisplay()), context.sqrlTable, field)) {
      return Optional.empty();
    }

    return Optional.of(GraphQLFieldDefinition.newFieldDefinition()
        .name(conformName(field.getName().getDisplay()))
        .type(wrap(getTypeReference(field.getToTable(), context.getNames()), field.getMultiplicity()))
        .arguments(field.accept(new ArgumentGenerator(), context))
        .build());
  }

  public static boolean logIfInvalid(boolean isValidTable, SQRLTable table) {
    if (!isValidTable) {
      log.warn("Skipping table in graphql schema, not a valid graphql name: {}", table.getName());
    }

    return isValidTable;
  }

  public static boolean logIfInvalid(boolean isValid, SQRLTable table, Field f) {
    if (!isValid) {
      log.warn("Skipping column in graphql schema, not a valid graphql name: {}:{}", table.getName(),
          f.getName().getDisplay());
    }
    return isValid;
  }

  public static boolean logIfInvalid(boolean isValid, SQRLTable table, FunctionParameter f) {
    if (!isValid) {
      log.warn("Skipping argument in graphql schema, not a valid graphql name: {}:{}", table.getName(),
          f.getName());
    }
    return isValid;
  }

}
