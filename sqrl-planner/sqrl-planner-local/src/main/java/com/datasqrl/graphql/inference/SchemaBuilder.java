/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.graphql.inference;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.calcite.SqrlRelBuilder;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.function.SqrlFunctionParameter;
import com.datasqrl.graphql.APIConnectorManager;
import com.datasqrl.graphql.inference.SchemaInferenceModel.InferredComputedField;
import com.datasqrl.graphql.inference.SchemaInferenceModel.InferredField;
import com.datasqrl.graphql.inference.SchemaInferenceModel.InferredFieldVisitor;
import com.datasqrl.graphql.inference.SchemaInferenceModel.InferredFunctionField;
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
import com.datasqrl.graphql.inference.SchemaInferenceModel.InferredSubscriptionObjectVisitor;
import com.datasqrl.graphql.inference.SchemaInferenceModel.InferredSubscriptions;
import com.datasqrl.graphql.inference.SchemaInferenceModel.NestedField;
import com.datasqrl.graphql.server.Model;
import com.datasqrl.graphql.server.Model.ArgumentLookupCoords;
import com.datasqrl.graphql.server.Model.ArgumentParameter;
import com.datasqrl.graphql.server.Model.Coords;
import com.datasqrl.graphql.server.Model.FieldLookupCoords;
import com.datasqrl.graphql.server.Model.MutationCoords;
import com.datasqrl.graphql.server.Model.RootGraphqlModel;
import com.datasqrl.graphql.server.Model.SourceParameter;
import com.datasqrl.graphql.server.Model.StringSchema;
import com.datasqrl.graphql.server.Model.SubscriptionCoords;
import com.datasqrl.graphql.server.Model.VariableArgument;
import com.datasqrl.graphql.util.ApiQueryBase;
import com.datasqrl.graphql.util.ApiQueryBase.ApiQueryBaseBuilder;
import com.datasqrl.plan.local.generate.SqrlQueryPlanner;
import com.datasqrl.plan.queries.APIQuery;
import com.datasqrl.plan.queries.APISource;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import graphql.language.InputValueDefinition;
import graphql.language.ListType;
import graphql.language.NonNullType;
import graphql.schema.GraphQLList;
import graphql.schema.idl.SchemaParser;
import graphql.schema.idl.TypeDefinitionRegistry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.apache.calcite.jdbc.SqrlSchema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.TableFunction;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.calcite.sql.validate.SqlUserDefinedTableFunction;
import org.apache.calcite.tools.RelBuilder;

public class SchemaBuilder implements
    InferredSchemaVisitor<RootGraphqlModel, Object>,
    InferredRootObjectVisitor<List<Coords>, Object>,
    InferredMutationObjectVisitor<List<MutationCoords>, Object>,
    InferredSubscriptionObjectVisitor<List<SubscriptionCoords>, Object>,
    InferredFieldVisitor<Coords, Object> {

  private final SqrlFramework framework;
  private final APISource source;
  private final TypeDefinitionRegistry registry;
  private final SqrlSchema schema;
  private final RelBuilder relBuilder;

  private final SqlOperatorTable operatorTable;

  private final APIConnectorManager apiManager;

  private final AtomicInteger queryCounter = new AtomicInteger();

  public SchemaBuilder(SqrlFramework framework, APISource source, SqrlSchema schema, RelBuilder relBuilder,
                              SqrlQueryPlanner planner,
                              SqlOperatorTable operatorTable, APIConnectorManager apiManager) {
    this.framework = framework;
    this.source = source;
    this.registry = (new SchemaParser()).parse(source.getSchemaDefinition());
    this.schema = schema;
    this.relBuilder = relBuilder;
    this.operatorTable = operatorTable;
    this.apiManager = apiManager;
  }

  @Override
  public RootGraphqlModel visitSchema(InferredSchema schema, Object context) {
    RootGraphqlModel.RootGraphqlModelBuilder builder = RootGraphqlModel.builder()
        .schema(StringSchema.builder().schema(source.getSchemaDefinition()).build())
        .coords(schema.getQuery().accept(this, context));

    schema.getMutation().map(m->
        builder.mutations(m.accept(this, context)));

    schema.getSubscription().map(m->
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
        .map(mut->new MutationCoords(mut.getName(), mut.getSinkConfig()))
        .collect(Collectors.toList());
  }

  @Override
  public List<SubscriptionCoords> visitSubscriptions(InferredSubscriptions rootObject,
      Object context) {
    return rootObject.getSubscriptions().stream()
        .map(sub->new SubscriptionCoords(sub.getName(), sub.getSinkConfig()))
        .collect(Collectors.toList());
  }

  @Override
  public Coords visitInterfaceField(InferredInterfaceField field, Object context) {
    throw new RuntimeException("Not supported yet");
  }

  @SneakyThrows
  @Override
  public Coords visitObjectField(InferredObjectField field, Object context) {
    List<String> currentPath;
    if (field.getParentTable() == null) {
      currentPath = field.getTable().getPath()
          .toStringList();
    } else {
      currentPath = field.getParentTable().getPath().concat(Name.system(field.getFieldDefinition().getName())).toStringList();
    }

    SqlUserDefinedTableFunction op = SqrlRelBuilder.getSqrlTableFunction(framework.getQueryPlanner(), currentPath);
    TableFunction function = op.getFunction();

    Set<String> limitOffset = Set.of("limit", "offset");

    List<List<InputValueDefinition>> argCombinations = generateCombinations(field.getFieldDefinition()
        .getInputValueDefinitions().stream()
        .filter(f->!limitOffset.contains(f.getName().toLowerCase()))
        .collect(Collectors.toList()));
    SqlNameMatcher matcher = framework.getCatalogReader().nameMatcher();

    ArgumentLookupCoords.ArgumentLookupCoordsBuilder coordsBuilder = ArgumentLookupCoords.builder()
        .parentType(field.getParent().getName())
        .fieldName(field.getFieldDefinition().getName());

    for (List<InputValueDefinition> arg : argCombinations) {
      SqrlRelBuilder builder = framework.getQueryPlanner().getSqrlRelBuilder();

      AtomicInteger uniqueOrdinal = new AtomicInteger(0);
      //Anticipate all args being found
      builder.functionScan(op, 0, function.getParameters().stream()
          .map(param -> new RexDynamicParam(param.getType(null), uniqueOrdinal.getAndIncrement()))
          .collect(Collectors.toList()));

      Model.ArgumentSet.ArgumentSetBuilder matchSet = Model.ArgumentSet.builder();
      ApiQueryBaseBuilder queryParams = ApiQueryBase.builder();

      //modifiable since we'll subtract the args we find
      Map<List<String>, InputValueDefinition> argMap = new HashMap<>(Maps.uniqueIndex(arg, n->List.of(n.getName())));

      //Iterate over all required args in the function
      for (FunctionParameter p : op.getFunction().getParameters()) {
        SqrlFunctionParameter parameter = (SqrlFunctionParameter) p;
        //check parameter is in the arg list, if so then dynamic param
        String paramName = parameter.getName().substring(1);
        InputValueDefinition def;
        if (parameter.isInternal()) {
//          int fieldIndex = matcher.indexOf(builder.peek().getRowType().getFieldNames(), paramName);
//          Preconditions.checkState(fieldIndex != -1);
//          String fieldName = builder.peek().getRowType().getFieldNames().get(fieldIndex);

          queryParams.parameter(new SourceParameter(paramName+"$0")); //todo get lhs
        } else if ((def = matcher.get(argMap, List.<String>of(), List.of(paramName))) != null) {
          argMap.remove(List.of(def.getName()));

//          int fieldIndex = matcher.indexOf(builder.peek().getRowType().getFieldNames(), p.getName());
//          Preconditions.checkState(fieldIndex != -1);
//          String fieldName = builder.peek().getRowType().getFieldNames().get(fieldIndex);

          queryParams.parameter(new ArgumentParameter(paramName));
          matchSet.argument(new VariableArgument(def.getName(), null));
            //param found,
        } else {
          //param not in list, attempt to get default value.

          throw new RuntimeException("Could not find param: " + p.getName());
        }
      }

      List<RexNode> conditions = new ArrayList<>();
      //For all remaining args, create equality conditions
      for (Map.Entry<List<String>, InputValueDefinition> args : argMap.entrySet()) {
        if (limitOffset.contains(args.getKey().get(0).toLowerCase())) {
          continue;
        }
        int fieldIndex = matcher.indexOf(builder.peek().getRowType().getFieldNames(), args.getKey().get(0));
        if (fieldIndex == -1) {
          throw new RuntimeException("Could not find field: " + args.getKey().get(0));
        }
        RexInputRef lhs = builder.field(fieldIndex);

        RexDynamicParam rhs = new RexDynamicParam(lhs.getType(), uniqueOrdinal.getAndIncrement());
        conditions.add(builder.equals(lhs, rhs));

        queryParams.parameter(new ArgumentParameter(args.getKey().get(0)));//todo: get name of lhs
        matchSet.argument(new VariableArgument(args.getKey().get(0), null));
      }
      if (!conditions.isEmpty()) {
        builder.filter(conditions);
      }
      //
      //add defaults
      RelNode relNode = builder.buildAndUnshadow();

      String nameId = field.getParent().getName() + "." + field.getFieldDefinition().getName() + "-" + queryCounter.incrementAndGet();
      APIQuery query = new APIQuery(nameId, relNode);
      apiManager.addQuery(query);
      matchSet.query(queryParams
          .relNode(relNode)
          .query(query)
          .build());
      Model.ArgumentSet argumentSet = matchSet.build();

      coordsBuilder.match(argumentSet);
    }

    ArgumentLookupCoords coords = coordsBuilder.build();

    //Assure all arg sets are unique
    Set s = new HashSet<>(coords.getMatchs());
    Preconditions.checkState(s.size() == coords.getMatchs().size(), "Duplicate arg sets:" + coords.getMatchs());

    return coords;
  }


  public static List<List<InputValueDefinition>> generateCombinations(List<InputValueDefinition> input) {
    List<List<InputValueDefinition>> result = new ArrayList<>();

    // Starting with an empty combination
    result.add(new ArrayList<>());

    for (InputValueDefinition definition : input) {
      List<List<InputValueDefinition>> newCombinations = new ArrayList<>();

      for (List<InputValueDefinition> existing : result) {
        // Handling NonNull or List<NonNull>
        if (definition.getType() instanceof NonNullType ||
            (definition.getType() instanceof ListType) &&
                (((GraphQLList) definition.getType()).getWrappedType() instanceof NonNullType)) {
          existing.add(definition);
          newCombinations.add(new ArrayList<>(existing));
        } else {
          // Without current item
          newCombinations.add(new ArrayList<>(existing));

          // With current item
          existing.add(definition);
          newCombinations.add(existing);
        }
      }

      result = newCombinations;
    }

    return result;
  }

  @Override
  public Coords visitComputedField(InferredComputedField field, Object context) {
    return null;
  }

  @Override
  public Coords visitScalarField(InferredScalarField field, Object context) {
    return FieldLookupCoords.builder()
        .parentType(field.getParent().getName())
        .fieldName(field.getFieldDefinition().getName())
        .columnName(field.getColumn().getVtName().getCanonical())
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

  @Override
  public Coords visitFunctionField(InferredFunctionField inferredFunctionField, Object context) {
    return null;
  }
}
