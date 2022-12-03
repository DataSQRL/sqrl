package com.datasqrl.graphql.inference;

import com.datasqrl.graphql.inference.SchemaInferenceModel.InferredComputedField;
import com.datasqrl.graphql.inference.SchemaInferenceModel.InferredFieldVisitor;
import com.datasqrl.graphql.inference.SchemaInferenceModel.InferredInterfaceField;
import com.datasqrl.graphql.inference.SchemaInferenceModel.InferredMutation;
import com.datasqrl.graphql.inference.SchemaInferenceModel.InferredObjectField;
import com.datasqrl.graphql.inference.SchemaInferenceModel.InferredPagedField;
import com.datasqrl.graphql.inference.SchemaInferenceModel.InferredQuery;
import com.datasqrl.graphql.inference.SchemaInferenceModel.InferredRootObjectVisitor;
import com.datasqrl.graphql.inference.SchemaInferenceModel.InferredScalarField;
import com.datasqrl.graphql.inference.SchemaInferenceModel.InferredSchema;
import com.datasqrl.graphql.inference.SchemaInferenceModel.InferredSchemaVisitor;
import com.datasqrl.graphql.inference.SchemaInferenceModel.InferredSubscription;
import com.datasqrl.graphql.inference.SchemaInferenceModel.NestedField;
import com.datasqrl.graphql.inference.argument.ArgumentHandler;
import com.datasqrl.graphql.inference.argument.ArgumentHandlerContextV1;
import com.datasqrl.graphql.inference.argument.EqHandler;
import com.datasqrl.graphql.inference.argument.LimitOffsetHandler;
import com.datasqrl.graphql.server.Model.ArgumentLookupCoords;
import com.datasqrl.graphql.server.Model.Coords;
import com.datasqrl.graphql.server.Model.FieldLookupCoords;
import com.datasqrl.graphql.server.Model.PgParameterHandler;
import com.datasqrl.graphql.server.Model.RootGraphqlModel;
import com.datasqrl.graphql.server.Model.SourcePgParameter;
import com.datasqrl.graphql.server.Model.StringSchema;
import com.datasqrl.graphql.util.ApiQueryBase;
import com.datasqrl.graphql.util.PagedApiQueryBase;
import com.datasqrl.plan.calcite.OptimizationStage;
import com.datasqrl.plan.calcite.Planner;
import com.datasqrl.plan.calcite.SqlValidatorUtil;
import com.datasqrl.plan.calcite.table.VirtualRelationalTable;
import com.datasqrl.plan.local.transpile.ConvertJoinDeclaration;
import com.datasqrl.plan.queries.APIQuery;
import com.datasqrl.schema.Relationship;
import com.datasqrl.schema.Relationship.JoinType;
import com.datasqrl.schema.SQRLTable;
import graphql.language.FieldDefinition;
import graphql.language.InputValueDefinition;
import graphql.language.NonNullType;
import graphql.language.ObjectTypeDefinition;
import graphql.schema.idl.SchemaParser;
import graphql.schema.idl.TypeDefinitionRegistry;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.Value;
import org.apache.calcite.jdbc.SqrlCalciteSchema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqrlJoinDeclarationSpec;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.tools.RelBuilder;

public class PgSchemaBuilder implements
    InferredSchemaVisitor<RootGraphqlModel, Object>,
    InferredRootObjectVisitor<List<Coords>, Object>,
    InferredFieldVisitor<Coords, Object> {

  private final String stringSchema;
  private final TypeDefinitionRegistry registry;
  private final SqrlCalciteSchema schema;
  private final Planner planner;
  private final RelBuilder relBuilder;
  //todo: migrate out
  List<ArgumentHandler> argumentHandlers = List.of(
      new EqHandler(), new LimitOffsetHandler()
  );
  @Getter
  private List<APIQuery> apiQueries = new ArrayList<>();

  public PgSchemaBuilder(String gqlSchema, SqrlCalciteSchema schema, RelBuilder relBuilder,
      Planner planner) {
    this.stringSchema = gqlSchema;
    this.registry = (new SchemaParser()).parse(gqlSchema);
    this.schema = schema;
    this.planner = planner;
    this.relBuilder = relBuilder;
  }

  @Override
  public RootGraphqlModel visitSchema(InferredSchema schema, Object context) {
    return RootGraphqlModel.builder()
        .schema(StringSchema.builder().schema(stringSchema).build())
        .coords(schema.getQuery().accept(this, context))
        .build();
  }

  @Override
  public List<Coords> visitQuery(InferredQuery rootObject, Object context) {
    return rootObject.getFields().stream()
        .map(f -> f.accept(this, rootObject.getQuery()))
        .collect(Collectors.toList());
  }

  @Override
  public List<Coords> visitMutation(InferredMutation rootObject, Object context) {
    throw new RuntimeException("Not supported yet");
  }

  @Override
  public List<Coords> visitSubscription(InferredSubscription rootObject, Object context) {
    throw new RuntimeException("Not supported yet");
  }

  @Override
  public Coords visitInterfaceField(InferredInterfaceField field, Object context) {
    throw new RuntimeException("Not supported yet");
  }

  @Override
  public Coords visitObjectField(InferredObjectField field, Object context) {
    RelNode relNode = relBuilder.scan(field.getTable().getVt().getNameId()).build();

    Set<ArgumentSet> possibleArgCombinations = createArgumentSuperset(
        field.getTable(),
        relNode, new ArrayList<>(),
        field.getFieldDefinition().getInputValueDefinitions(),
        field.getFieldDefinition());

    //Todo: Project out only the needed columns. This requires visiting all it's children so we know
    // what other fields they need

    return buildArgumentQuerySet(possibleArgCombinations,
        field.getParent(),
        field.getFieldDefinition(), new ArrayList<>());
  }

  //Creates a superset of all possible arguments w/ their respective query
  private Set<ArgumentSet> createArgumentSuperset(SQRLTable sqrlTable,
      RelNode relNode, List<PgParameterHandler> existingHandlers,
      List<InputValueDefinition> inputArgs, FieldDefinition fieldDefinition) {
    //todo: table functions

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

  private boolean hasAnyRequiredArgs(List<InputValueDefinition> args) {
    //Todo: also check for table functions
    return args.stream()
        .filter(a -> a.getType() instanceof NonNullType)
        .findAny()
        .isPresent();
  }

  private ArgumentLookupCoords buildArgumentQuerySet(Set<ArgumentSet> possibleArgCombinations,
      ObjectTypeDefinition parent, FieldDefinition fieldDefinition,
      List<PgParameterHandler> existingHandlers) {
    ArgumentLookupCoords.ArgumentLookupCoordsBuilder coordsBuilder = ArgumentLookupCoords.builder()
        .parentType(parent.getName()).fieldName(fieldDefinition.getName());

    for (ArgumentSet argumentSet : possibleArgCombinations) {
      //Add api query
      RelNode relNode = optimize(argumentSet.getRelNode());
      APIQuery query = new APIQuery(UUID.randomUUID().toString(), relNode);

      apiQueries.add(query);

      List<PgParameterHandler> argHandler = new ArrayList<>();
      argHandler.addAll(existingHandlers);
      argHandler.addAll(argumentSet.getArgumentParameters());

      if (argumentSet.isLimitOffsetFlag()) {
        coordsBuilder.match(com.datasqrl.graphql.server.Model.ArgumentSet.builder()
            .arguments(argumentSet.getArgumentHandlers()).query(
                PagedApiQueryBase.builder().query(query).relNode(argumentSet.getRelNode())
                    .relAndArg(argumentSet)
                    .parameters(argHandler)
                    .build()).build()).build();
      } else {
        coordsBuilder.match(com.datasqrl.graphql.server.Model.ArgumentSet.builder()
            .arguments(argumentSet.getArgumentHandlers()).query(
                ApiQueryBase.builder().query(query).relNode(argumentSet.getRelNode())
                    .relAndArg(argumentSet)
                    .parameters(argHandler)
                    .build()).build()).build();
      }
    }
    return coordsBuilder.build();
  }

  private RelNode optimize(RelNode relNode) {
    return this.planner.transform(OptimizationStage.PUSH_DOWN_FILTERS, relNode);
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
        .columnName(field.getColumn().getShadowedName().getCanonical())
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
    //If there's a join declaration, add source handlers for all _. fields
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
      List<PgParameterHandler> handlers = new ArrayList<>();
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
        handlers.add(new SourcePgParameter(pkName));
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

      List<PgParameterHandler> handlers = new ArrayList<>();
      for (int i = 0; i < formTable.getVt().getPrimaryKeyNames().size(); i++) {
        RelDataTypeField field = relBuilder.peek().getRowType().getFieldList()
            .get(i);
        RexDynamicParam dynamicParam = relBuilder.getRexBuilder()
            .makeDynamicParam(field.getType(),
                handlers.size());
        builder = relBuilder.filter(relBuilder.getRexBuilder().makeCall(SqlStdOperatorTable.EQUALS,
            relBuilder.getRexBuilder().makeInputRef(relBuilder.peek(), field.getIndex()),
            dynamicParam));
        handlers.add(new SourcePgParameter(formTable.getVt().getPrimaryKeyNames().get(i)));
      }
      return new RelPair(builder.build(), handlers);
    }
  }

  private SqlNode toQuery(SqrlJoinDeclarationSpec spec, VirtualRelationalTable vt) {
    ConvertJoinDeclaration convert = new ConvertJoinDeclaration(Optional.of(vt));
    return spec.accept(convert);
  }

  private RelNode plan(SqlNode node) {
    SqlValidator sqrlValidator = SqlValidatorUtil.createSqlValidator(schema,
        List.of());
    SqlNode validated = sqrlValidator.validate(node);

    planner.setValidator(validated, sqrlValidator);
    return planner.rel(validated).rel;
  }

  @Value
  public static class FieldContext {
    ObjectTypeDefinition parent;
  }
  @Value
  public static class RelPair {

    RelNode relNode;
    List<PgParameterHandler> handlers;
  }
}
