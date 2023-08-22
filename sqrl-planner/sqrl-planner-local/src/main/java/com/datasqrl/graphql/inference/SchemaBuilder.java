/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.graphql.inference;

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
import com.datasqrl.graphql.inference.SchemaInferenceModel.InferredSubscriptions;
import com.datasqrl.graphql.inference.SchemaInferenceModel.NestedField;
import com.datasqrl.graphql.inference.argument.ArgumentHandler;
import com.datasqrl.graphql.inference.argument.ArgumentHandlerContextV1;
import com.datasqrl.graphql.inference.argument.EqHandler;
import com.datasqrl.graphql.inference.argument.LimitOffsetHandler;
import com.datasqrl.graphql.server.Model;
import com.datasqrl.graphql.server.Model.ArgumentLookupCoords;
import com.datasqrl.graphql.server.Model.FieldLookupCoords;
import com.datasqrl.graphql.server.Model.JdbcParameterHandler;
import com.datasqrl.graphql.server.Model.MutationCoords;
import com.datasqrl.graphql.server.Model.QueryBase;
import com.datasqrl.graphql.server.Model.Coords;
import com.datasqrl.graphql.server.Model.RootGraphqlModel;
import com.datasqrl.graphql.server.Model.SourceParameter;
import com.datasqrl.graphql.server.Model.StringSchema;
import com.datasqrl.graphql.server.Model.SubscriptionCoords;
import com.datasqrl.graphql.util.ApiQueryBase;
import com.datasqrl.graphql.util.PagedApiQueryBase;
import com.datasqrl.plan.*;
import com.datasqrl.plan.local.generate.SqrlQueryPlanner;
import com.datasqrl.plan.local.generate.TableFunctionBase;
import com.datasqrl.plan.local.transpile.ConvertJoinDeclaration;
import com.datasqrl.plan.queries.APIQuery;
import com.datasqrl.plan.queries.APISource;
import com.datasqrl.plan.table.VirtualRelationalTable;
import com.datasqrl.schema.Relationship;
import com.datasqrl.schema.Relationship.JoinType;
import com.datasqrl.schema.SQRLTable;
import com.datasqrl.schema.TypeFactory;
import graphql.language.FieldDefinition;
import graphql.language.InputValueDefinition;
import graphql.language.NonNullType;
import graphql.language.ObjectTypeDefinition;
import graphql.schema.idl.SchemaParser;
import graphql.schema.idl.TypeDefinitionRegistry;

import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import lombok.SneakyThrows;
import lombok.Value;
import org.apache.calcite.DataContext;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.SqrlSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.*;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.schema.impl.TableFunctionImpl;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqrlJoinDeclarationSpec;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.tools.RelBuilder;
import org.apache.flink.table.planner.calcite.FlinkCalciteSqlValidator;

public class SchemaBuilder implements
    InferredSchemaVisitor<RootGraphqlModel, Object>,
    InferredRootObjectVisitor<List<Coords>, Object>,
    InferredMutationObjectVisitor<List<MutationCoords>, Object>,
    InferredSubscriptionObjectVisitor<List<SubscriptionCoords>, Object>,
    InferredFieldVisitor<Coords, Object> {

  private final APISource source;
  private final TypeDefinitionRegistry registry;
  private final SqrlSchema schema;
  private final RelBuilder relBuilder;

  private final SqlOperatorTable operatorTable;

  private final SqrlQueryPlanner planner;
  //todo: migrate out
  List<ArgumentHandler> argumentHandlers = List.of(
      new EqHandler(), new LimitOffsetHandler()
  );
  private final APIConnectorManager apiManager;

  private final AtomicInteger queryCounter = new AtomicInteger();

  public SchemaBuilder(APISource source, SqrlSchema schema, RelBuilder relBuilder,
                       SqrlQueryPlanner planner,
                       SqlOperatorTable operatorTable, APIConnectorManager apiManager) {
    this.source = source;
    this.registry = (new SchemaParser()).parse(source.getSchemaDefinition());
    this.schema = schema;
    this.relBuilder = relBuilder;
    this.operatorTable = operatorTable;
    this.planner = planner;
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
    Set<ArgumentSet> possibleArgCombinations;

    if (hasMatchingTableFunction(schema, field)) {
      TableFunctionBase b = (TableFunctionBase)schema.getFunctions(field.getFieldDefinition().getName(), false)
          .stream().findFirst().get();

      RelDataTypeFactory factory = schema.getCluster().getTypeFactory();
      AtomicInteger i = new AtomicInteger();
      List<RexNode> rexNodes = b.getParameters().stream()
          .map(p->p.getType(factory))
          .map(t->relBuilder.getRexBuilder()
              .makeDynamicParam(t, i.getAndIncrement()))
          .collect(Collectors.toList());

      String params = rexNodes.stream()
          .map(e-> "?")
          .collect(Collectors.joining(","));

      RelNode relNode = planner.plan(SqlParser.create(
          String.format("SELECT * FROM table(\"%s\"(%s))", field.getFieldDefinition().getName(), params)).parseQuery());

      FunctionParameter p = b.getParameters().get(0);
      Set<Model.Argument> newHandlers = new LinkedHashSet<>();
      newHandlers.add(Model.VariableArgument.builder().path(p.getName()).build());

      List<Model.ArgumentParameter> parameters = new ArrayList<>();
      parameters.add(Model.ArgumentParameter.builder().path(p.getName()).build());

      possibleArgCombinations = Set.of(new ArgumentSet(relNode,
          newHandlers, parameters, false));
    } else {
      RelNode relNode = relBuilder
          .scan(field.getTable().getVt().getNameId())
//        .project(projectedColumns)
          .build();

      possibleArgCombinations = createArgumentSuperset(
          field.getTable(),
          relNode, new ArrayList<>(),
          field.getFieldDefinition().getInputValueDefinitions(),
          field.getFieldDefinition());

      //Todo: Project out only the needed columns. This requires visiting all it's children so we know
      // what other fields they need
    }
    return buildArgumentQuerySet(possibleArgCombinations,
        field.getParent(),
        field.getFieldDefinition(), new ArrayList<>());
  }

  private boolean hasMatchingTableFunction(SqrlSchema schema, InferredObjectField field) {
    return schema.getFunctionNames().contains(
        field.getFieldDefinition().getName())
        /*TODO: also has matching arguments*/;
  }

  //Creates a superset of all possible arguments w/ their respective query
  private Set<ArgumentSet> createArgumentSuperset(SQRLTable sqrlTable,
      RelNode relNode, List<JdbcParameterHandler> existingHandlers,
      List<InputValueDefinition> inputArgs, FieldDefinition fieldDefinition) {
    /**
     * 1. Enumerate table functions and match with graphql (root and join declarations)
     *
     */

    Set<ArgumentSet> args = new HashSet<>();
    //TODO: remove if not used by final query set (has required params)
    args.add(new ArgumentSet(relNode, new LinkedHashSet<>(), new ArrayList<>(), false));

    for (InputValueDefinition arg : inputArgs) {
      boolean handled = false;
      for (ArgumentHandler handler : argumentHandlers) {
        ArgumentHandlerContextV1 contextV1 = new ArgumentHandlerContextV1(arg, args, sqrlTable,
            relBuilder,
            existingHandlers);
        if (handler.canHandle(contextV1)) {
          args = handler.accept(contextV1);
          handled = true;
          break;
        }
      }
      if (!handled) {
        throw new RuntimeException(String.format("Unhandled Arg : %s in %s", arg, fieldDefinition));
      }
    }

    return args;
  }

  private ArgumentLookupCoords buildArgumentQuerySet(Set<ArgumentSet> possibleArgCombinations,
                                                     ObjectTypeDefinition parent, FieldDefinition fieldDefinition,
                                                     List<JdbcParameterHandler> existingHandlers) {
    ArgumentLookupCoords.ArgumentLookupCoordsBuilder coordsBuilder = ArgumentLookupCoords.builder()
        .parentType(parent.getName()).fieldName(fieldDefinition.getName());

    for (ArgumentSet argumentSet : possibleArgCombinations) {
      //Add api query
      RelNode relNode = optimize(argumentSet.getRelNode());
      String nameId = parent.getName() + "." + fieldDefinition.getName() + "-" + queryCounter.incrementAndGet();
      APIQuery query = new APIQuery(nameId, relNode);
      apiManager.addQuery(query);

      List<JdbcParameterHandler> argHandler = new ArrayList<>();
      argHandler.addAll(existingHandlers);
      argHandler.addAll(argumentSet.getArgumentParameters());
      QueryBase queryBase;
      if (argumentSet.isLimitOffsetFlag()) {
        queryBase = PagedApiQueryBase.builder()
            .query(query)
            .relNode(argumentSet.getRelNode())
            .relAndArg(argumentSet)
            .parameters(argHandler)
            .build();
      } else {
        queryBase = ApiQueryBase.builder()
            .query(query)
            .relNode(argumentSet.getRelNode())
            .relAndArg(argumentSet)
            .parameters(argHandler)
            .build();
      }

      //Add argument set to coords
      coordsBuilder.match(com.datasqrl.graphql.server.Model.ArgumentSet.builder()
          .arguments(argumentSet.getArgumentHandlers()).query(queryBase).build()).build();
    }
    return coordsBuilder.build();
  }

  private RelNode optimize(RelNode relNode) {
    return planner.runStage(OptimizationStage.PUSH_DOWN_FILTERS,
        relNode);
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

    RelPair relPair = createNestedRelNode(field.getRelationship());

    Set<ArgumentSet> possibleArgCombinations = createArgumentSuperset(
        objectField.getTable(),
        relPair.getRelNode(),
        relPair.getHandlers(),
        objectField.getFieldDefinition().getInputValueDefinitions(),
        objectField.getFieldDefinition());

    //Todo: Project out only the needed columns. This requires visiting all it's children so we know
    // what other fields they need

    return buildArgumentQuerySet(possibleArgCombinations,
        objectField.getParent(),
        objectField.getFieldDefinition(), relPair.getHandlers());
  }

  private RelPair createNestedRelNode(Relationship r) {
    //If there's a join declaration, add source handlers for all @. fields
    //Add default sorting
    if (r.getJoin().isPresent()) {
      SqrlJoinDeclarationSpec spec = r.getJoin().get();
      //Todo: build a more concise plan.
      // There could be other column references to the SELF relation (like in the ORDER or in
      //  a join condition that would have to be extracted into the where clause after conversion to
      //  a CNF.
      SqlNode query = toQuery(spec, r.getFromTable().getVt());

      RelNode relNode = plan(query);

      RelBuilder builder = relBuilder.push(relNode);
      List<JdbcParameterHandler> handlers = new ArrayList<>();
      List<String> primaryKeyNames = r.getFromTable().getVt().getPrimaryKeyNames();
      for (int i = 0; i < primaryKeyNames.size(); i++) {
        String pkName = primaryKeyNames.get(i);
        //by convention: the primary key field is the first n fields
        RelDataTypeField field = relBuilder.peek().getRowType().getFieldList().get(i);
        RexDynamicParam dynamicParam = relBuilder.getRexBuilder()
            .makeDynamicParam(field.getType(),
                handlers.size());
        builder = relBuilder.filter(relBuilder.getRexBuilder().makeCall(SqlStdOperatorTable.EQUALS,
            relBuilder.getRexBuilder().makeInputRef(relBuilder.peek(), field.getIndex()),
            dynamicParam));
        handlers.add(new SourceParameter(pkName));
      }
      return new RelPair(builder.build(), handlers);
    } else {
      RelBuilder builder = relBuilder.scan(r.getToTable().getVt().getNameId());
      SQRLTable formTable = null;
      if (r.getJoinType() == JoinType.PARENT) {
        formTable = r.getToTable();
      } else if (r.getJoinType() == JoinType.CHILD) {
        formTable = r.getFromTable();
      } else {
        throw new RuntimeException("Unknown join type");
      }

      List<JdbcParameterHandler> handlers = new ArrayList<>();
      for (int i = 0; i < formTable.getVt().getPrimaryKeyNames().size(); i++) {
        RelDataTypeField field = relBuilder.peek().getRowType().getFieldList()
            .get(i);
        RexDynamicParam dynamicParam = relBuilder.getRexBuilder()
            .makeDynamicParam(field.getType(),
                handlers.size());
        builder = relBuilder.filter(relBuilder.getRexBuilder().makeCall(SqlStdOperatorTable.EQUALS,
            relBuilder.getRexBuilder().makeInputRef(relBuilder.peek(), field.getIndex()),
            dynamicParam));
        handlers.add(new SourceParameter(formTable.getVt().getPrimaryKeyNames().get(i)));
      }
      return new RelPair(builder.build(), handlers);
    }
  }

  private SqlNode toQuery(SqrlJoinDeclarationSpec spec, VirtualRelationalTable vt) {
    ConvertJoinDeclaration convert = new ConvertJoinDeclaration(Optional.of(vt));
    return spec.accept(convert);
  }

  private RelNode plan(SqlNode node) {
    return planner.planQuery(node);
  }

  @Value
  public static class FieldContext {

    ObjectTypeDefinition parent;
  }

  @Value
  public static class RelPair {

    RelNode relNode;
    List<JdbcParameterHandler> handlers;
  }
}
