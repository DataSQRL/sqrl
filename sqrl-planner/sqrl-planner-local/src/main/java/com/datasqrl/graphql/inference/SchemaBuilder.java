/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.graphql.inference;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.config.SerializedSqrlConfig;
import com.datasqrl.graphql.APIConnectorManager;
import com.datasqrl.graphql.inference.SchemaInferenceModel.InferredComputedField;
import com.datasqrl.graphql.inference.SchemaInferenceModel.InferredField;
import com.datasqrl.graphql.inference.SchemaInferenceModel.InferredFieldVisitor;
import com.datasqrl.graphql.inference.SchemaInferenceModel.InferredInterfaceField;
import com.datasqrl.graphql.inference.SchemaInferenceModel.InferredMutationObjectVisitor;
import com.datasqrl.graphql.inference.SchemaInferenceModel.InferredMutations;
import com.datasqrl.graphql.inference.SchemaInferenceModel.InferredObjectField;
import com.datasqrl.graphql.inference.SchemaInferenceModel.InferredPagedField;
import com.datasqrl.graphql.inference.SchemaInferenceModel.InferredQuery;
import com.datasqrl.graphql.inference.SchemaInferenceModel.InferredRootObjectVisitor;
import com.datasqrl.graphql.inference.SchemaInferenceModel.InferredScalarField;
import com.datasqrl.graphql.inference.SchemaInferenceModel.InferredSchema;
import com.datasqrl.graphql.inference.SchemaInferenceModel.InferredSchemaVisitor;
import com.datasqrl.graphql.inference.SchemaInferenceModel.InferredSubscription;
import com.datasqrl.graphql.inference.SchemaInferenceModel.InferredSubscriptionObjectVisitor;
import com.datasqrl.graphql.inference.SchemaInferenceModel.InferredSubscriptionScalarField;
import com.datasqrl.graphql.inference.SchemaInferenceModel.InferredSubscriptions;
import com.datasqrl.graphql.inference.SchemaInferenceModel.NestedField;
import com.datasqrl.graphql.server.Model;
import com.datasqrl.graphql.server.Model.ArgumentLookupCoords;
import com.datasqrl.graphql.server.Model.Coords;
import com.datasqrl.graphql.server.Model.FieldLookupCoords;
import com.datasqrl.graphql.server.Model.MutationCoords;
import com.datasqrl.graphql.server.Model.RootGraphqlModel;
import com.datasqrl.graphql.server.Model.StringSchema;
import com.datasqrl.graphql.server.Model.SubscriptionCoords;
import com.datasqrl.io.tables.TableSource;
import com.datasqrl.plan.queries.APISource;
import com.datasqrl.plan.queries.APISubscription;
import graphql.language.InputValueDefinition;
import graphql.language.NonNullType;
import graphql.language.Value;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SchemaBuilder implements
    InferredSchemaVisitor<RootGraphqlModel, Object>,
    InferredRootObjectVisitor<List<Coords>, Object>,
    InferredMutationObjectVisitor<List<MutationCoords>, Object>,
    InferredSubscriptionObjectVisitor<List<SubscriptionCoords>, Object>,
    InferredFieldVisitor<Coords, Object> {

  private final APISource source;
  private final APIConnectorManager apiManager;

  public SchemaBuilder(APISource source, APIConnectorManager apiManager) {
    this.source = source;
    this.apiManager = apiManager;
  }

  @Override
  public RootGraphqlModel visitSchema(InferredSchema schema, Object context) {
    RootGraphqlModel.RootGraphqlModelBuilder builder = RootGraphqlModel.builder()
        .schema(StringSchema.builder().schema(source.getSchemaDefinition()).build())
        .coords(schema.getQuery().accept(this, context));

    schema.getMutation().map(m ->
        builder.mutations(m.accept(this, context)));

    schema.getSubscription().map(m ->
        builder.subscriptions(m.accept(this, context)));

    if (schema.getSubscription().isPresent()) {
      InferredSubscriptions s = schema.getSubscription().get();
      for (InferredField field : s.getFields()) {
        builder.coord(field.accept(this, context));
      }
    }

    return builder.build();
  }

  @Override
  public List<Coords> visitQuery(InferredQuery rootObject, Object context) {
    return rootObject.getFields().stream()
        .map(f -> f.accept(this, rootObject.getQuery()))
        .collect(Collectors.toList());
  }

  @Override
  public List<MutationCoords> visitMutation(InferredMutations rootObject, Object context) {
    return rootObject.getMutations().stream()
        .map(mut -> new MutationCoords(mut.getName(), mut.getSinkConfig()))
        .collect(Collectors.toList());
  }

  @Override
  public List<SubscriptionCoords> visitSubscriptions(InferredSubscriptions subscription,
      Object context) {
    return subscription.getSubscriptions().stream()
        .map(sub -> new SubscriptionCoords(sub.getName(), createConfig(sub), sub.getFilters()))
        .collect(Collectors.toList());
  }

  public SerializedSqrlConfig createConfig(InferredSubscription subscription) {
    APISubscription apiSubscription = new APISubscription(Name.system(subscription.getName()),
        subscription.getSource());
    TableSource tableSource = apiManager.addSubscription(apiSubscription, subscription.getTable());

    return tableSource.getConfiguration().getConfig().serialize();
  }

  @Override
  public Coords visitInterfaceField(InferredInterfaceField field, Object context) {
    throw new RuntimeException("Not supported yet");
  }

  @SneakyThrows
  @Override
  public Coords visitObjectField(InferredObjectField field, Object context) {
    ArgumentLookupCoords.ArgumentLookupCoordsBuilder coordsBuilder = ArgumentLookupCoords.builder()
        .parentType(field.getParent().getName())
        .fieldName(field.getFieldDefinition().getName());

    for (Model.ArgumentSet argumentSet : field.getArgumentSets()) {
      coordsBuilder.match(argumentSet);
    }

    return coordsBuilder.build();
  }

  public static List<List<ArgCombination>> generateCombinations(
      List<InputValueDefinition> input) {
    List<List<ArgCombination>> result = new ArrayList<>();

    // Starting with an empty combination
    result.add(new ArrayList<>());

    for (InputValueDefinition definition : input) {
      List<List<ArgCombination>> newCombinations = new ArrayList<>();

      for (List<ArgCombination> existing : result) {

        if (definition.getDefaultValue() != null) { // A variable or the default value
          //TODO: include default value
//          List<ArgCombination> withDefault = new ArrayList<>(existing);
//          withDefault.add(new ArgCombination(definition, Optional.of(definition.getDefaultValue())));
//          newCombinations.add(withDefault);

          List<ArgCombination> withVariable = new ArrayList<>(existing);
          withVariable.add(new ArgCombination(definition, Optional.empty()));
          newCombinations.add(withVariable);
        } else if (definition.getType() instanceof NonNullType) { // Always provided by user
          existing.add(new ArgCombination(definition, Optional.empty()));
          newCombinations.add(new ArrayList<>(existing));
        } else {
          // Without current item
          newCombinations.add(new ArrayList<>(existing));

          // With current item
          existing.add(new ArgCombination(definition, Optional.empty()));
          newCombinations.add(existing);
        }
      }

      result = newCombinations;
    }

    return result;
  }

  @lombok.Value
  public static class ArgCombination {
    InputValueDefinition definition;
    Optional<Value> defaultValue;
  }

  @Override
  public Coords visitComputedField(InferredComputedField field, Object context) {
    return null;
  }

  @Override
  public Coords visitSubscriptionScalarField(InferredSubscriptionScalarField field,
      Object context) {
    return FieldLookupCoords.builder()
        .parentType(field.getParent().getName())
        .fieldName(field.getFieldDefinition().getName())
        .columnName(field.getColumn().getName().getDisplay())
        .build();
  }

  @Override
  public Coords visitScalarField(InferredScalarField field, Object context) {
    return FieldLookupCoords.builder()
        .parentType(field.getParent().getName())
        .fieldName(field.getFieldDefinition().getName())
        .columnName(field.getColumn().getVtName())
        .build();
  }

  @Override
  public Coords visitPagedField(InferredPagedField field, Object context) {
    return null;
  }

  @Override
  public Coords visitNestedField(NestedField field, Object context) {
    InferredObjectField objectField = (InferredObjectField) field.getInferredField();
    return visitObjectField(objectField, context);
  }
}
