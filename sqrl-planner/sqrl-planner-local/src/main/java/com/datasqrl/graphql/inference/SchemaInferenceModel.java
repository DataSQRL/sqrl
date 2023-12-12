/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.graphql.inference;

import com.datasqrl.calcite.function.SqrlTableMacro;
import com.datasqrl.config.SerializedSqrlConfig;
import com.datasqrl.graphql.inference.SqrlSchemaForInference.*;
import com.datasqrl.graphql.server.Model;
import com.datasqrl.plan.queries.APISource;
import graphql.language.FieldDefinition;
import graphql.language.ObjectTypeDefinition;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import lombok.ToString;
import lombok.Value;

/**
 * Only does the schema inference. - Creates a set of objects that are the schema inference objects
 * - Easier to test that its correct, just a tree of objects - Moves iteration concerns to a static
 * model - Less conditions in SQL building
 */
public class SchemaInferenceModel {

  @Value
  @ToString
  public static class InferredSchema {
    String name;

    InferredQuery query;
    Optional<InferredMutations> mutation;
    Optional<InferredSubscriptions> subscription;

    public <R, C> R accept(InferredSchemaVisitor<R, C> visitor, C context) {
      return visitor.visitSchema(this, context);
    }
  }

  public interface InferredSchemaVisitor<R, C> {

    public R visitSchema(InferredSchema schema, C context);
  }

  public interface InferredRootObject {

  }

  @Value
  @ToString
  public static class InferredQuery implements InferredRootObject {

    ObjectTypeDefinition query;
    List<InferredField> fields;

    public <R, C> R accept(InferredRootObjectVisitor<R, C> visitor, C context) {
      return visitor.visitQuery(this, context);
    }
  }

  @Value
  @ToString
  public static class InferredMutations implements InferredRootObject {
    List<InferredMutation> mutations;

    public <R, C> R accept(InferredMutationObjectVisitor<R, C> visitor, C context) {
      return visitor.visitMutation(this, context);
    }
  }

  @Value
  @ToString
  public static class InferredMutation {
    String name;
    SerializedSqrlConfig sinkConfig;
  }

  @Value
  @ToString
  public static class InferredSubscriptions implements InferredRootObject {
    List<InferredSubscription> subscriptions;
    List<InferredField> fields;
    public <R, C> R accept(InferredSubscriptionObjectVisitor<R, C> visitor, C context) {
      return visitor.visitSubscriptions(this, context);
    }
  }

  @Value
  public static class InferredSubscription {
    String name;
    SQRLTable table;
    APISource source;
    Map<String, String> filters;
  }

  public interface InferredRootObjectVisitor<R, C> {

    R visitQuery(InferredQuery rootObject, C context);
  }

  public interface InferredMutationObjectVisitor<R, C> {
    R visitMutation(InferredMutations rootObject, C context);
  }

  public interface InferredSubscriptionObjectVisitor<R, C> {
    R visitSubscriptions(InferredSubscriptions rootObject, C context);
  }
  public interface InferredField {

    public <R, C> R accept(InferredFieldVisitor<R, C> visitor, C context);
  }

  @Value
  @ToString
  public static class NestedField implements InferredField {

    Relationship relationship;
    InferredField inferredField;

    @Override
    public <R, C> R accept(InferredFieldVisitor<R, C> visitor, C context) {
      return visitor.visitNestedField(this, context);
    }
  }

  //Also used for unions
  @Value
  @ToString
  public static class InferredInterfaceField implements InferredField {

    FieldDefinition fieldDefinition;
    //All types must have concrete classes
    List<InferredObjectField> inferredTypes;

    @Override
    public <R, C> R accept(InferredFieldVisitor<R, C> visitor, C context) {
      return visitor.visitInterfaceField(this, context);
    }
  }

  @Value
  @ToString
  public static class InferredObjectField implements InferredField {

    SQRLTable parentTable;
    ObjectTypeDefinition parent;
    FieldDefinition fieldDefinition;
    ObjectTypeDefinition objectTypeDefinition;
    SQRLTable table;
    SqrlTableMacro macro;
    List<Model.ArgumentSet> argumentSets;

    @Override
    public <R, C> R accept(InferredFieldVisitor<R, C> visitor, C context) {
      return visitor.visitObjectField(this, context);
    }
  }

  @Value
  @ToString
  public static class InferredComputedField implements InferredField {

    //allows new fields like a composite ID field.
    @Override
    public <R, C> R accept(InferredFieldVisitor<R, C> visitor, C context) {
      return visitor.visitComputedField(this, context);
    }
  }

  @Value
  @ToString
  public static class InferredScalarField implements InferredField {

    FieldDefinition fieldDefinition;
    Column column;
    ObjectTypeDefinition parent;

    @Override
    public <R, C> R accept(InferredFieldVisitor<R, C> visitor, C context) {
      return visitor.visitScalarField(this, context);
    }
  }

  @Value
  @ToString
  public static class InferredSubscriptionScalarField implements InferredField {

    FieldDefinition fieldDefinition;
    Column column;
    ObjectTypeDefinition parent;

    @Override
    public <R, C> R accept(InferredFieldVisitor<R, C> visitor, C context) {
      return visitor.visitSubscriptionScalarField(this, context);
    }
  }

  @Value
  @ToString
  public static class InferredPagedField implements InferredField {

    @Override
    public <R, C> R accept(InferredFieldVisitor<R, C> visitor, C context) {
      return visitor.visitPagedField(this, context);
    }
  }

  public interface InferredFieldVisitor<R, C> {

    R visitInterfaceField(InferredInterfaceField field, C context);

    R visitObjectField(InferredObjectField field, C context);

    R visitComputedField(InferredComputedField field, C context);

    R visitScalarField(InferredScalarField field, C context);
    R visitSubscriptionScalarField(InferredSubscriptionScalarField field, C context);

    R visitPagedField(InferredPagedField field, C context);

    R visitNestedField(NestedField field, C context);
  }
}
