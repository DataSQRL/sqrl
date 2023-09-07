/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.graphql.inference;

import com.datasqrl.calcite.Dialect;
import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.calcite.SqrlRelBuilder;
import com.datasqrl.calcite.schema.SqrlListUtil;
import com.datasqrl.calcite.schema.SqrlTableFunction;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.function.SqrlFunctionParameter;
import com.datasqrl.function.SqrlInternalFunctionParameter;
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
import com.datasqrl.graphql.inference.argument.ArgumentHandler;
import com.datasqrl.graphql.inference.argument.ArgumentHandlerContextV1;
import com.datasqrl.graphql.inference.argument.EqHandler;
import com.datasqrl.graphql.inference.argument.LimitOffsetHandler;
import com.datasqrl.graphql.server.Model;
import com.datasqrl.graphql.server.Model.ArgumentLookupCoords;
import com.datasqrl.graphql.server.Model.Coords;
import com.datasqrl.graphql.server.Model.FieldLookupCoords;
import com.datasqrl.graphql.server.Model.JdbcParameterHandler;
import com.datasqrl.graphql.server.Model.MutationCoords;
import com.datasqrl.graphql.server.Model.QueryBase;
import com.datasqrl.graphql.server.Model.RootGraphqlModel;
import com.datasqrl.graphql.server.Model.SourceParameter;
import com.datasqrl.graphql.server.Model.StringSchema;
import com.datasqrl.graphql.server.Model.SubscriptionCoords;
import com.datasqrl.graphql.util.ApiQueryBase;
import com.datasqrl.graphql.util.PagedApiQueryBase;
import com.datasqrl.plan.local.generate.SqrlQueryPlanner;
import com.datasqrl.plan.queries.APIQuery;
import com.datasqrl.plan.queries.APISource;
import com.datasqrl.plan.table.VirtualRelationalTable;
import com.datasqrl.schema.Relationship;
import com.datasqrl.schema.Relationship.JoinType;
import com.datasqrl.schema.SQRLTable;
import com.google.common.base.Preconditions;
import graphql.language.FieldDefinition;
import graphql.language.InputValueDefinition;
import graphql.language.ObjectTypeDefinition;
import graphql.schema.idl.SchemaParser;
import graphql.schema.idl.TypeDefinitionRegistry;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.Value;
import org.apache.calcite.jdbc.SqrlSchema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.TableFunction;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlUserDefinedTableFunction;
import org.apache.calcite.tools.RelBuilder;
import org.apache.commons.collections.ListUtils;

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

  //todo: migrate out
  List<ArgumentHandler> argumentHandlers = List.of(
      new EqHandler(), new LimitOffsetHandler()
  );
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
  /**
   * Do permutation path if there are no non-internal arguments on the sqrl function
   *
   * If there are arguments on the sqrl function:
   * 1.
   *
   *
   */
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

    System.out.println(field.getFieldDefinition().getName() + " " + field.getParent().getName());
    List<FunctionParameter> userProvidedSqrlArgs = function.getParameters().stream().filter(f->!((SqrlFunctionParameter)f).isInternal())
        .collect(Collectors.toList());

    //allow variable args for undefined params
    if (userProvidedSqrlArgs.size() == 0) {
//      Preconditions.checkState(function.getParameters().size() == 0, "Can only permute base tables");
      SqrlRelBuilder builder = framework.getQueryPlanner().getSqrlRelBuilder();

      //add internal args to function
      List<RexNode> args = function.getParameters().stream()
          .map(param -> new RexDynamicParam(param.getType(null), param.getOrdinal()))
          .collect(Collectors.toList());

      builder.functionScan(op, 0, args);
      RelNode relNode = builder.buildAndUnshadow();

      Set<ArgumentSet> possibleArgCombinations = createArgumentSuperset(
          field.getTable(),
          relNode, new ArrayList<>(),
          field.getFieldDefinition().getInputValueDefinitions(),
          field.getFieldDefinition());

      //add source parameters
      List<Model.JdbcParameterHandler> parameters = function.getParameters().stream()
          .map(p->
              (((SqrlFunctionParameter)p).isInternal())
                  ? SourceParameter.builder()
                  .key(p.getName().substring(1))
                  .build()
                  :
                      Model.ArgumentParameter.builder()
                          .path(p.getName().substring(1))
                          .build())
          .collect(Collectors.toList());

      //Todo: Project out only the needed columns. This requires visiting all it's children so we know
      // what other fields they need
      ArgumentLookupCoords argumentLookupCoords = buildArgumentQuerySet(possibleArgCombinations,
          field.getParent(),
          field.getFieldDefinition(), parameters);
      System.out.println( "Generated Query: " + framework.getQueryPlanner().relToString(Dialect.CALCITE, relNode));
      System.out.println(argumentLookupCoords.getMatchs());
      return argumentLookupCoords;
    }


    SqrlRelBuilder builder = framework.getQueryPlanner().getSqrlRelBuilder();

    //todo
    List<InputValueDefinition> defs = field.getFieldDefinition().getInputValueDefinitions();

    List<RexNode> args = function.getParameters().stream()
        .map(param -> new RexInputRef(param.getOrdinal(), param.getType(null)))
        .collect(Collectors.toList());

    builder.functionScan(op, 0, args);

    Set<Model.Argument> newHandlers = function.getParameters().stream()
        .filter(f->!((SqrlFunctionParameter)f).isInternal())
        .map(p->
            Model.VariableArgument.builder()
            .path(p.getName().substring(1))
            .build())
        .collect(Collectors.toSet());

    List<Model.JdbcParameterHandler> parameters = function.getParameters().stream()
        .map(p->
             (((SqrlFunctionParameter)p).isInternal())
                 ? SourceParameter.builder()
                     .key(p.getName().substring(1))
                         .build()
                 :
            Model.ArgumentParameter.builder()
            .path(p.getName().substring(1))
            .build())
        .collect(Collectors.toList());

    RelNode relNode = builder.buildAndUnshadow();

    System.out.println( "Generated Query: " + framework.getQueryPlanner().relToString(Dialect.CALCITE, relNode));
    System.out.println(newHandlers);
    System.out.println(parameters);

    ArgumentLookupCoords.ArgumentLookupCoordsBuilder coordsBuilder = ArgumentLookupCoords.builder()
        .parentType(field.getParent().getName()).fieldName(field.getFieldDefinition().getName());

    String nameId = field.getParent().getName() + "." + field.getFieldDefinition().getName() + "-" + queryCounter.incrementAndGet();
    APIQuery query = new APIQuery(nameId, relNode);
    apiManager.addQuery(query);

    ArgumentSet argSet = new ArgumentSet(relNode, newHandlers, parameters, false);

    QueryBase base = ApiQueryBase.builder()
        .query(query)
        .relNode(relNode)
        .relAndArg(argSet)
        .parameters(parameters)
        .build();

    return coordsBuilder
        .match(Model.ArgumentSet.builder()
          .arguments(argSet.getArgumentHandlers())
          .query(base)
          .build())
        .build();

//      RelNode relNode = relBuilder
//          .scan(((VirtualRelationalTable)field.getTable().getVt()).getNameId())
////        .project(projectedColumns)
//          .build();
//
//      possibleArgCombinations = createArgumentSuperset(
//          field.getTable(),
//          relNode, new ArrayList<>(),
//          field.getFieldDefinition().getInputValueDefinitions(),
//          field.getFieldDefinition());
//
//      //Todo: Project out only the needed columns. This requires visiting all it's children so we know
//      // what other fields they need
//    }
//    return buildArgumentQuerySet(possibleArgCombinations,
//        field.getParent(),
//        field.getFieldDefinition(), new ArrayList<>());
  }

  private boolean hasMatchingTableFunction(SqrlSchema schema, InferredObjectField field) {
    return schema.getFunctionNames().contains(
        Name.system(field.getFieldDefinition().getName()).getCanonical() + "$")
        /*TODO: also has matching arguments*/;
  }

  //Creates a superset of all possible arguments w/ their respective query
  private Set<ArgumentSet> createArgumentSuperset(SQRLTable sqrlTable,
      RelNode relNode, List<JdbcParameterHandler> existingHandlers,
      List<InputValueDefinition> inputArgs, FieldDefinition fieldDefinition) {
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
      RelNode relNode = argumentSet.getRelNode();
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

//  private RelNode optimize(RelNode relNode) {
//    return planner.runStage(OptimizationStage.PUSH_DOWN_FILTERS,
//        relNode);
//  }

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
//    RelPair relPair = createNestedRelNode(field.getRelationship());
//
//    Set<ArgumentSet> possibleArgCombinations = createArgumentSuperset(
//        objectField.getTable(),
//        relPair.getRelNode(),
//        relPair.getHandlers(),
//        objectField.getFieldDefinition().getInputValueDefinitions(),
//        objectField.getFieldDefinition());

    //Todo: Project out only the needed columns. This requires visiting all it's children so we know
    // what other fields they need

//    return buildArgumentQuerySet(possibleArgCombinations,
//        objectField.getParent(),
//        objectField.getFieldDefinition(), relPair.getHandlers());
  }

  @Override
  public Coords visitFunctionField(InferredFunctionField inferredFunctionField, Object context) {
    return null;
  }

  private RelPair createNestedRelNode(Relationship r) {
    //If there's a join declaration, add source handlers for all @. fields
    //Add default sorting
    NamePath path = r.getFromTable().getPath().concat(r.getName());
    List<String> pathStr = path.toStringList();

    SqrlTableFunction tableFunctionMacro =(SqrlTableFunction) SqrlRelBuilder.getSqrlTableFunction(framework.getQueryPlanner(),
        pathStr).getFunction();

    if (tableFunctionMacro != null) {

      SqlNode query = tableFunctionMacro.getNode();

      RelNode relNode = plan(query);

      RelBuilder builder = relBuilder.push(relNode);
      List<JdbcParameterHandler> handlers = new ArrayList<>();
//      handlers.add(new SourceParameter(pkName));
//      for (int i = 0; i < tableFunctionMacro.getInternalFields().size(); i++) {
//        String pkName = tableFunctionMacro.getInternalFields().get(i);
//        by convention: the primary key field is the first n fields
//        RelDataTypeField field = relBuilder.peek().getRowType().getFieldList().get(i);
//        RexDynamicParam dynamicParam = relBuilder.getRexBuilder()
//            .makeDynamicParam(field.getType(),
//                handlers.size());
//        builder = relBuilder.filter(relBuilder.getRexBuilder().makeCall(SqlStdOperatorTable.EQUALS,
//            relBuilder.getRexBuilder().makeInputRef(relBuilder.peek(), field.getIndex()),
//            dynamicParam));
//        handlers.add(new SourceParameter(pkName));
//      }
      return new RelPair(builder.build(), handlers);
    } else {
      throw new RuntimeException("Here");
//
//      RelBuilder builder = relBuilder.scan(((VirtualRelationalTable)r.getToTable().getVt()).getNameId());
//      SQRLTable formTable = null;
//      if (r.getJoinType() == JoinType.PARENT) {
//        formTable = r.getToTable();
//      } else if (r.getJoinType() == JoinType.CHILD) {
//        formTable = r.getFromTable();
//      } else {
//        throw new RuntimeException("Unknown join type");
//      }
//
//      List<JdbcParameterHandler> handlers = new ArrayList<>();
//      for (int i = 0; i < ((VirtualRelationalTable)formTable.getVt()).getPrimaryKeyNames().size(); i++) {
//        RelDataTypeField field = relBuilder.peek().getRowType().getFieldList()
//            .get(i);
//        RexDynamicParam dynamicParam = relBuilder.getRexBuilder()
//            .makeDynamicParam(field.getType(),
//                handlers.size());
//        builder = relBuilder.filter(relBuilder.getRexBuilder().makeCall(SqlStdOperatorTable.EQUALS,
//            relBuilder.getRexBuilder().makeInputRef(relBuilder.peek(), field.getIndex()),
//            dynamicParam));
//        handlers.add(new SourceParameter(((VirtualRelationalTable)formTable.getVt()).getPrimaryKeyNames().get(i)));
//      }
//      return new RelPair(builder.build(), handlers);
    }
  }

  private RelNode plan(SqlNode node) {
    return framework.getQueryPlanner().plan(Dialect.CALCITE, node);
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
