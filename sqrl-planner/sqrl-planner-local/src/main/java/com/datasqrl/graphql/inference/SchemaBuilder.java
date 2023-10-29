/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.graphql.inference;

import com.datasqrl.calcite.Dialect;
import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.calcite.function.SqrlTableMacro;
import com.datasqrl.function.SqrlFunctionParameter;
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
import com.datasqrl.graphql.inference.SchemaInferenceModel.InferredSubscriptionObjectVisitor;
import com.datasqrl.graphql.inference.SchemaInferenceModel.InferredSubscriptionScalarField;
import com.datasqrl.graphql.inference.SchemaInferenceModel.InferredSubscriptions;
import com.datasqrl.graphql.inference.SchemaInferenceModel.NestedField;
import com.datasqrl.graphql.server.Model;
import com.datasqrl.graphql.server.Model.Argument;
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
import com.datasqrl.graphql.util.PagedApiQueryBase;
import com.datasqrl.plan.local.generate.SqrlQueryPlanner;
import com.datasqrl.plan.queries.APIQuery;
import com.datasqrl.plan.queries.APISource;
import com.datasqrl.util.CalciteUtil;
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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlNameMatcher;
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
  private final SqrlSchemaForInference schema;
  private final RelBuilder relBuilder;

  private final SqlOperatorTable operatorTable;

  private final APIConnectorManager apiManager;

  private final AtomicInteger queryCounter = new AtomicInteger();

  public SchemaBuilder(SqrlFramework framework, APISource source, SqrlSchemaForInference schema,
      RelBuilder relBuilder,
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
  public List<SubscriptionCoords> visitSubscriptions(InferredSubscriptions rootObject,
      Object context) {
    return rootObject.getSubscriptions().stream()
        .map(sub -> new SubscriptionCoords(sub.getName(), sub.getSinkConfig(), sub.getFilters()))
        .collect(Collectors.toList());
  }

  @Override
  public Coords visitInterfaceField(InferredInterfaceField field, Object context) {
    throw new RuntimeException("Not supported yet");
  }

  @SneakyThrows
  @Override
  public Coords visitObjectField(InferredObjectField field, Object context) {
    SqrlTableMacro function = field.getMacro();

    List<SqlOperator> matches = new ArrayList<>();
    framework.getSqrlOperatorTable()
        .lookupOperatorOverloads(new SqlIdentifier(function.getDisplayName(), SqlParserPos.ZERO),
            SqlFunctionCategory.USER_DEFINED_TABLE_FUNCTION, SqlSyntax.FUNCTION, matches,
            framework.getCatalogReader().nameMatcher());

    Set<String> limitOffset = Set.of("limit", "offset");

    List<List<InputValueDefinition>> argCombinations = generateCombinations(
        field.getFieldDefinition()
            .getInputValueDefinitions());
    SqlNameMatcher matcher = framework.getCatalogReader().nameMatcher();

    ArgumentLookupCoords.ArgumentLookupCoordsBuilder coordsBuilder = ArgumentLookupCoords.builder()
        .parentType(field.getParent().getName())
        .fieldName(field.getFieldDefinition().getName());

    for (List<InputValueDefinition> arg : argCombinations) {
      List<InputValueDefinition> accounting = new ArrayList<>(arg);

      //TODO- This whole block is wrong
      // Requirements:
      // 1. There can be multiple tables
      // 2. There can be default parameters
      // 3. We must disambiguate calls
      //
      // We could just generate them via relbuilder blindly?
      // All internal parameters are provided by the scope
      // All non-internal parameters are provided by the user
      // we should be able to just generate it? no?
      // Table(orders(?, ?)

      RelBuilder builder = framework.getQueryPlanner().getRelBuilder();
      final AtomicInteger uniqueOrdinal = new AtomicInteger(0);

      Map<List<String>, InputValueDefinition> argMap = new HashMap<>(
          Maps.uniqueIndex(arg, n -> List.of(n.getName())));

      List<RexNode> params = function.getParameters().stream()
          .map(param -> getDynamicParamOrDefault((SqrlFunctionParameter) param, argMap,
              uniqueOrdinal, matcher, relBuilder.getRexBuilder()))
          .collect(Collectors.toList());
      builder.functionScan(matches.get(0), 0, params);

      Model.ArgumentSet.ArgumentSetBuilder matchSet = Model.ArgumentSet.builder();
      ApiQueryBaseBuilder queryParams = ApiQueryBase.builder();

      //Check if we have any user provided args for permutation use case
//      List<FunctionParameter> userProvidedArgs = function.getParameters().stream()
//          .filter(f -> !((SqrlFunctionParameter) f).isInternal())
//          .collect(Collectors.toList());
//
      //fill in the args in the same way as above
      int i = 0;
      for (FunctionParameter functionParameter : function.getParameters()) {
        SqrlFunctionParameter parameter = (SqrlFunctionParameter) functionParameter;
        //check parameter is in the arg list, if so then dynamic param
        InputValueDefinition def;
        if (parameter.isInternal()) {
          RelDataType rowType = field.getParentTable().getRelOptTable().getRowType(null);

          String fieldName = rowType.getFieldList().get(i++)
              .getName(); //Note: this is wrong, need to know keys?
          //Rough check
          System.out.println(fieldName + ":" + parameter.getName());
          if (!fieldName.endsWith(parameter.getName()) //worst hack in the world
              && !parameter.getName().endsWith(fieldName)) {
            int i1 = matcher.indexOf(rowType.getFieldNames(), parameter.getVariableName());
            fieldName = rowType.getFieldList().get(i1).getName();
          }
          queryParams.parameter(new SourceParameter(fieldName));

          Preconditions.checkState(fieldName.endsWith(parameter.getName())
              || parameter.getName().endsWith(fieldName));
        } else if ((def = matcher.get(argMap, List.of(), List.of(parameter.getVariableName())))
            != null) {
          accounting.remove(def);
          argMap.remove(List.of(def.getName()));
          queryParams.parameter(new ArgumentParameter(parameter.getVariableName()));
          matchSet.argument(new VariableArgument(def.getName(), null));
        } else {
          //param not in list, attempt to get default value.
          throw new RuntimeException("Could not find param: " + parameter.getName());
        }
      }
      boolean limitOffsetFlag = false;

      List<RexNode> extraConditions = new ArrayList<>();
      if (!accounting.isEmpty()) {
        for (InputValueDefinition def : accounting) {
          if (limitOffset.contains(def.getName().toLowerCase())) {
            matchSet.argument(VariableArgument.builder()
                .path(def.getName().toLowerCase())
                .build());
            limitOffsetFlag = true;
            continue;
          }
          int fieldIndex = matcher.indexOf(builder.peek().getRowType().getFieldNames(), def.getName());
          if (fieldIndex == -1) {
            throw new RuntimeException("Could not find field: " + def.getName());
          }
          RexInputRef lhs = builder.field(fieldIndex);

          RexDynamicParam rhs = new RexDynamicParam(lhs.getType(), uniqueOrdinal.getAndIncrement());
          extraConditions.add(builder.equals(lhs, rhs));

          queryParams.parameter(new ArgumentParameter(def.getName()));//todo: get name of lhs
          matchSet.argument(new VariableArgument(def.getName(), null));
        }
      }

      if (!extraConditions.isEmpty()) {
        builder.filter(extraConditions);
      }

      RelNode relNode = framework.getQueryPlanner().expandMacros(builder.build());

      String nameId = field.getParent().getName() + "." + field.getFieldDefinition().getName() + "-"
          + queryCounter.incrementAndGet();
      APIQuery query = new APIQuery(nameId, relNode);
      apiManager.addQuery(query);
      if (limitOffsetFlag) {
        ApiQueryBase base = queryParams.build();
      //convert
      //todo: limit offset strategy is bad
        matchSet.query(PagedApiQueryBase.builder()
            .parameters(base.getParameters())
            .query(query)
            .relNode(relNode).build());
      } else {
      matchSet.query(queryParams
          .relNode(relNode)
          .query(query)
          .build());
      }
      Model.ArgumentSet argumentSet = matchSet.build();

      Set<RexNode> dynamicParams = new HashSet<>();
      CalciteUtil.applyRexShuttleRecursively(relNode, new RexShuttle() {
        @Override
        public RexNode visitDynamicParam(RexDynamicParam dynamicParam) {
          dynamicParams.add(dynamicParam);
          return super.visitDynamicParam(dynamicParam);
        }
      });

      if (argumentSet.getQuery() instanceof ApiQueryBase) {
        int size = ((ApiQueryBase) argumentSet.getQuery())
            .getParameters().size();

        Preconditions.checkState(dynamicParams.size() == size,
            "Args not matching");
      }
      System.out.println(argumentSet);
      System.out.println(framework.getQueryPlanner().relToString(Dialect.CALCITE, relNode));

      coordsBuilder.match(argumentSet);
    }

    ArgumentLookupCoords coords = coordsBuilder.build();


    Set<Set<Argument>> seen = new HashSet();
    for (Model.ArgumentSet c : coords.getMatchs()) {
      if (seen.contains(c.getArguments())) {
        throw new RuntimeException("Duplicate argument matches");
      }

      seen.add(c.getArguments());

    }
    //Assure all arg sets are unique
    Set s = new HashSet<>(coords.getMatchs());
    Preconditions.checkState(s.size() == coords.getMatchs().size(),
        "Duplicate arg sets:" + coords.getMatchs());

    return coords;
  }

  private RexNode getDynamicParamOrDefault(SqrlFunctionParameter param,
      Map<List<String>, InputValueDefinition> argMap, AtomicInteger uniqueOrdinal,
      SqlNameMatcher matcher, RexBuilder rexBuilder) {
    if (param.getDefaultValue().isPresent() && matcher.get(argMap, List.of(),
        List.of(param.getVariableName())) == null) {
      SqlLiteral literal = (SqlLiteral) param.getDefaultValue().get();
      return rexBuilder.makeLiteral(literal.getValue(), param.getRelDataType(), true);
    }

    return new RexDynamicParam(param.getType(null), uniqueOrdinal.getAndIncrement());
  }


  public static List<List<InputValueDefinition>> generateCombinations(
      List<InputValueDefinition> input) {
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
