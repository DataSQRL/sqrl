/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.graphql.generate;

import com.datasqrl.graphql.inference.SqrlSchemaForInference;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLNonNull;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLScalarType;
import graphql.schema.GraphQLSchema;
import graphql.schema.GraphQLType;
import graphql.schema.GraphQLTypeReference;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Creates a default graphql schema based on the SQRL schema
 */
public class SchemaGenerator {
  List<GraphQLFieldDefinition> queryFields = new ArrayList<>();
  List<GraphQLObjectType> objectTypes = new ArrayList<>();

  public GraphQLSchema generate(SqrlSchemaForInference schema) {
    SchemaGeneratorContext context = new SchemaGeneratorContext();
    schema.accept(new QueryTypeGenerator(queryFields), context);
    schema.accept(new ObjectTypeGenerator(objectTypes), context);

    postProcess();

    if (queryFields.isEmpty()) {
      throw new RuntimeException("No tables found to build schema");
    }

    GraphQLObjectType query = GraphQLObjectType.newObject()
        .name("Query")
        .fields(queryFields)
        .build();

    return GraphQLSchema.newSchema()
        .query(query)
        .additionalTypes(new LinkedHashSet<>(objectTypes))
        .build();
  }


  void postProcess() {
    // Ensure every field points to a valid type
    Iterator<GraphQLObjectType> iterator = objectTypes.iterator();
    while (iterator.hasNext()) {
      GraphQLObjectType objectType = iterator.next();
      List<GraphQLFieldDefinition> invalidFields = new ArrayList<>();

      for (GraphQLFieldDefinition field : objectType.getFields()) {
        if (!isValidType(field.getType())) {
          invalidFields.add(field);
        }
      }

      // Refactor to remove invalid fields
      List<GraphQLFieldDefinition> fields = new ArrayList<>(objectType.getFields());
      fields.removeAll(invalidFields);

      // After removing invalid fields, if an object has no fields, it should be removed
      if (fields.isEmpty()) {
        iterator.remove();
      }
    }

    queryFields.removeIf(field -> !isValidType(field.getType()));

    // Ensure each object has at least one field
    objectTypes.removeIf(objectType -> objectType.getFields().isEmpty());
  }

  boolean isValidType(GraphQLType type) {
    type = unbox(type);
    // You can expand this logic depending on the intricacies of type validation
    if (type instanceof GraphQLTypeReference) {
      GraphQLTypeReference typeReference = (GraphQLTypeReference)type;
      for (GraphQLObjectType objectType : this.objectTypes) {
        if (typeReference.getName().equalsIgnoreCase(objectType.getName())) {
          return true;
        }
      }
    }

    return isBaseGraphQLType(type);
  }

  private GraphQLType unbox(GraphQLType type) {
    if (type instanceof GraphQLNonNull) {
      return unbox(((GraphQLNonNull) type).getWrappedType());
    } else if (type instanceof GraphQLList) {
      return unbox(((GraphQLList) type).getWrappedType());
    }
    return type;
  }

  public static boolean isValidGraphQLName(String name) {
    return Pattern.matches("[_A-Za-z][_0-9A-Za-z]*", name);
  }
  boolean isBaseGraphQLType(GraphQLType type) {
    return type instanceof GraphQLScalarType;
  }
}