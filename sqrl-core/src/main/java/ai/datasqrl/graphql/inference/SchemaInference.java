package ai.datasqrl.graphql.inference;

import ai.datasqrl.graphql.inference.argument.ArgumentHandler;
import ai.datasqrl.graphql.inference.argument.ArgumentHandlerContextV1;
import ai.datasqrl.graphql.inference.argument.EqHandler;
import ai.datasqrl.graphql.inference.argument.LimitOffsetHandler;
import ai.datasqrl.graphql.server.Model.ArgumentLookupCoords;
import ai.datasqrl.graphql.server.Model.ArgumentPgParameter;
import ai.datasqrl.graphql.server.Model.FieldLookupCoords;
import ai.datasqrl.graphql.server.Model.PgParameterHandler;
import ai.datasqrl.graphql.server.Model.Root;
import ai.datasqrl.graphql.server.Model.Root.RootBuilder;
import ai.datasqrl.graphql.server.Model.SourcePgParameter;
import ai.datasqrl.graphql.server.Model.StringSchema;
import ai.datasqrl.graphql.util.ApiQueryBase;
import ai.datasqrl.graphql.util.PagedApiQueryBase;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.plan.calcite.Planner;
import ai.datasqrl.plan.calcite.TranspilerFactory;
import ai.datasqrl.plan.calcite.table.VirtualRelationalTable;
import ai.datasqrl.plan.local.transpile.ConvertJoinDeclaration;
import ai.datasqrl.plan.queries.APIQuery;
import ai.datasqrl.schema.Column;
import ai.datasqrl.schema.Field;
import ai.datasqrl.schema.Relationship;
import ai.datasqrl.schema.Relationship.JoinType;
import ai.datasqrl.schema.Relationship.Multiplicity;
import ai.datasqrl.schema.RootJoinSQRLTable;
import ai.datasqrl.schema.SQRLTable;
import ai.datasqrl.schema.UnionSQRLTable;
import graphql.language.FieldDefinition;
import graphql.language.InputValueDefinition;
import graphql.language.InterfaceTypeDefinition;
import graphql.language.ListType;
import graphql.language.NonNullType;
import graphql.language.ObjectTypeDefinition;
import graphql.language.Type;
import graphql.language.TypeDefinition;
import graphql.schema.idl.SchemaParser;
import graphql.schema.idl.TypeDefinitionRegistry;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
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
import org.apache.flink.calcite.shaded.com.google.common.base.Preconditions;

@Getter
public class SchemaInference {

  private final TypeDefinitionRegistry registry;
  private final SqrlCalciteSchema schema;
  private final Planner planner;
  private final RootBuilder root;
  List<ArgumentHandler> argumentHandlers = List.of(
      new EqHandler(), new LimitOffsetHandler()
  );
  RelBuilder relBuilder;
  private List<APIQuery> apiQueries = new ArrayList<>();
  private Set<FieldDefinition> visited = new HashSet<>();

  public SchemaInference(String gqlSchema, SqrlCalciteSchema schema, RelBuilder relBuilder,
      Planner planner) {
    this.registry = (new SchemaParser()).parse(gqlSchema);
    this.schema = schema;
    this.planner = planner;
    Root.RootBuilder root = Root.builder()
        .schema(StringSchema.builder().schema(gqlSchema).build());
    this.root = root;
    this.relBuilder = relBuilder;
  }
  //Handles walking the schema completely

  public void accept() {

    //resolve additional types
    resolveTypes();

    Optional<TypeDefinition> query = registry.getType("Query");
    query.ifPresent(q -> resolveQueries((ObjectTypeDefinition) q));

    Optional<TypeDefinition> mutation = registry.getType("Mutation");
    mutation.ifPresent(m -> resolveMutations((ObjectTypeDefinition)m));

    Optional<TypeDefinition> subscription = registry.getType("subscription");
    subscription.ifPresent(s -> resolveSubscriptions((ObjectTypeDefinition)s));
  }

  private void resolveTypes() {
    //todo custom types: walk all defined types and import them
  }

  private void resolveQueries(ObjectTypeDefinition query) {
    //Walk each root type and resolve sqrl table
    //Order of operations:
    //1. Resolve SQRL Table
    for (FieldDefinition fieldDefinition : query.getFieldDefinitions()) {
      resolveQueryFromSchema(fieldDefinition, query);
    }
    //2. Delegate control to sqrl table to resolve

    //Union SQRL table
    //1. If type is union, match unions, type,

  }

  private void resolveQueryFromSchema(FieldDefinition fieldDefinition, ObjectTypeDefinition parent) {
    Optional<SQRLTable> sqrlTable = Optional.ofNullable(schema.getTable(fieldDefinition.getName(), false))
        .filter(t->t.getTable() instanceof SQRLTable)
        .map(t->(SQRLTable)t.getTable());
    Preconditions.checkState(sqrlTable.isPresent(),
        "Could not find associated SQRL type for field {}", fieldDefinition.getName());
    SQRLTable table = sqrlTable.get();
    resolveQuery(fieldDefinition, parent, table, Optional.empty());
  }

  private void resovleQueryFromRel(FieldDefinition fieldDefinition, ObjectTypeDefinition parent,
      Relationship relationship) {
    resolveQuery(fieldDefinition, parent, relationship.getToTable(), Optional.of(relationship));
  }

  private void resolveQuery(FieldDefinition fieldDefinition, ObjectTypeDefinition parent, SQRLTable table,
      Optional<Relationship> relationship) {
    if (table instanceof UnionSQRLTable) {
      resolveQuery((UnionSQRLTable) table, fieldDefinition, parent, relationship);
    } else if (table instanceof RootJoinSQRLTable) {
      resolveQuery((RootJoinSQRLTable) table, fieldDefinition, parent, relationship);
    } else {
      resolveQuery(table, fieldDefinition, parent, relationship);
    }
  }

  @Getter
  public class QueryContext {
    SQRLTable sqrlTable;
    FieldDefinition fieldDefinition;
    ObjectTypeDefinition parent;

    Multiplicity multiplicity;
    private final List<InputValueDefinition> args;

    public QueryContext(SQRLTable sqrlTable, FieldDefinition fieldDefinition,
        ObjectTypeDefinition parent, Multiplicity multiplicity, List<InputValueDefinition> args) {
      this.sqrlTable = sqrlTable;
      this.fieldDefinition = fieldDefinition;
      this.parent = parent;
      this.multiplicity = multiplicity;
      this.args = args;
    }
  }

  private void resolveQuery(SQRLTable sqrlTable, FieldDefinition fieldDefinition,
      ObjectTypeDefinition parent, Optional<Relationship> relationship) {
    QueryContext context = createQueryContext(sqrlTable, fieldDefinition, parent,
        (isListType(fieldDefinition.getType()) ? Multiplicity.MANY : Multiplicity.ONE),
        fieldDefinition.getInputValueDefinitions());

    TypeDefinition typeDef = unwrapObjectType(fieldDefinition.getType());

    if (typeDef instanceof InterfaceTypeDefinition) {
      // if interface: find super types and return all that match all fields.
      //   If one then resolve as object type, else throw.
      //   Need to add addl graphql resolver associated with type.
//    } else if (typeDef instanceof UnionTypeDefinition) {
    } else if (typeDef instanceof ObjectTypeDefinition) {
      ObjectTypeDefinition objectType = (ObjectTypeDefinition) typeDef;
      boolean allowed = isStructurallySubset(sqrlTable.getFields().getAccessibleFields(), objectType.getFieldDefinitions());
      Preconditions.checkState(allowed, "Type not allowed, has additional fields not seen in sqrl");

      resolveObjectTypeDef(context, sqrlTable, objectType, fieldDefinition, parent, relationship);
    } else {
      throw new RuntimeException("Could not infer query type");
    }

    //Now we also layer on the argument matching

    //We need to be associating PgParameterHandler with Indexes

  }

  private void resolveObjectTypeDef(QueryContext context, SQRLTable sqrlTable,
      ObjectTypeDefinition objectType, FieldDefinition fieldDefinition, ObjectTypeDefinition parent,
      Optional<Relationship> relationship) {
    //Resolve entire type before we walk relations
    //a. check if type was visited, if so then set column name constraints to account for shadowing
    //Resolve initial rel node, add table function args
    RelPair initial = relationship.map(r->createNestedRelNode(r))
        .orElseGet(()->createRelNode(context, sqrlTable));

    //Add field args
    // Dynamic params are given an index that correspond to RelAndArg.argumentHandlers
    Set<ArgumentSet> possibleArgCombinations = createArgumentSuperset(context, sqrlTable, objectType, initial);


    //Add parent context args
    // none for root query

    //Todo: Project out only the needed columns. This requires visiting all it's children so we know
    // what other fields they need

    ArgumentLookupCoords queries = buildArgumentQuerySet(possibleArgCombinations, parent, fieldDefinition, initial);

    root.coord(queries);

    visited.add(fieldDefinition);
    //6. walk relationships

    //Filter object type for scalar types
    // visit each field, check for type casting (this is the last project step)
    for (FieldDefinition definition : objectType.getFieldDefinitions()) {
      if (visited.contains(definition)) continue;
      Optional<Field> sqrlField = sqrlTable.getField(Name.system(definition.getName()));
      if (sqrlField.isPresent() && sqrlField.get() instanceof Relationship) {
        resovleQueryFromRel(definition, objectType, (Relationship)sqrlField.get());
      } else if (sqrlField.isPresent() && sqrlField.get() instanceof Column) {
        resolveField(definition, sqrlField.get());
      }
    }
  }

  private void resolveField(FieldDefinition definition, Field field) {
    FieldLookupCoords scalarCoord = FieldLookupCoords.builder()
        .parentType(definition.getName())
        .fieldName(field.getName().getCanonical())
        .columnName(((Column)field).getShadowedName().getCanonical())
        .build();
    root.coord(scalarCoord);
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
      SQRLTable destTable = null;
      if (r.getJoinType() == JoinType.PARENT) {
        destTable = r.getToTable();
      } else if (r.getJoinType() == JoinType.CHILD) {
        destTable = r.getFromTable();
      } else {
        throw new RuntimeException("Unknown join type");
      }

      List<PgParameterHandler> handlers = new ArrayList<>();
      for (String pkName : destTable.getVt().getPrimaryKeyNames()) {
        RelDataTypeField field = relBuilder.peek().getRowType()
            .getField(pkName, false, false);
        RexDynamicParam dynamicParam = relBuilder.getRexBuilder()
            .makeDynamicParam(field.getType(),
                handlers.size());
        builder = relBuilder.filter(relBuilder.getRexBuilder().makeCall(SqlStdOperatorTable.EQUALS,
            relBuilder.getRexBuilder().makeInputRef(relBuilder.peek(), field.getIndex()), dynamicParam));
        handlers.add(new SourcePgParameter(pkName));
      }
      return new RelPair(builder.build(), handlers);
    }
  }

  private SqlNode toQuery(SqrlJoinDeclarationSpec spec, VirtualRelationalTable vt) {
    ConvertJoinDeclaration convert = new ConvertJoinDeclaration(Optional.of(vt));
    return spec.accept(convert);
  }

  private RelNode plan(SqlNode node) {
    SqlValidator sqrlValidator = TranspilerFactory.createSqrlValidator(schema,
        List.of(), true);
    System.out.println(node);
    SqlNode validated = sqrlValidator.validate(node);

    planner.setValidator(validated, sqrlValidator);
    return planner.rel(validated).rel;
  }

  private ArgumentLookupCoords buildArgumentQuerySet(Set<ArgumentSet> possibleArgCombinations,
      ObjectTypeDefinition parent, FieldDefinition fieldDefinition, RelPair initial) {
    ArgumentLookupCoords.ArgumentLookupCoordsBuilder coordsBuilder = ArgumentLookupCoords.builder()
        .parentType(parent.getName()).fieldName(fieldDefinition.getName());

    for (ArgumentSet argumentSet : possibleArgCombinations) {
      //Add api query
      APIQuery query = new APIQuery(UUID.randomUUID().toString(), argumentSet.getRelNode());
      apiQueries.add(query);

      List<PgParameterHandler> argHandler = new ArrayList<>();
      argHandler.addAll(initial.handlers);
      argHandler.addAll(argumentSet.getArgumentHandlers().stream()
          .map(a -> ArgumentPgParameter.builder().path(a.getPath()).build())
          .collect(Collectors.toList()));

      //5. create coords matcher

      if (argumentSet.isLimitOffsetFlag()) {
        coordsBuilder.match(ai.datasqrl.graphql.server.Model.ArgumentSet.builder().arguments(argumentSet.getArgumentHandlers()).query(
            PagedApiQueryBase.builder().query(query).relNode(argumentSet.getRelNode()).relAndArg(argumentSet)
                .parameters(argHandler)
                .build()).build()).build();
      } else {
        coordsBuilder.match(ai.datasqrl.graphql.server.Model.ArgumentSet.builder().arguments(argumentSet.getArgumentHandlers()).query(
            ApiQueryBase.builder().query(query).relNode(argumentSet.getRelNode()).relAndArg(argumentSet)
                .parameters(argHandler)
                .build()).build()).build();
      }
    }
    return coordsBuilder.build();
  }

  //Creates a superset of all possible arguments w/ their respective query
  private Set<ArgumentSet> createArgumentSuperset(QueryContext context, SQRLTable sqrlTable,
      ObjectTypeDefinition objectType, RelPair initial) {
    //todo: table functions

    Set<ArgumentSet> args = new HashSet<>();
    if (!hasAnyRequiredArgs(context, sqrlTable)) {
      args.add(new ArgumentSet(initial.relNode, new LinkedHashSet<>(), false));
    }

    for (InputValueDefinition arg : context.getArgs()) {
      boolean handled = false;
      for (ArgumentHandler handler : argumentHandlers) {
        ArgumentHandlerContextV1 contextV1 = new ArgumentHandlerContextV1(arg, args, sqrlTable, relBuilder,
            initial.handlers);
        if (handler.canHandle(contextV1)) {
          args = handler.accept(contextV1);
          handled = true;
          break;
        }
      }
      if (!handled) {
        throw new RuntimeException(String.format("Unhandled Arg : %s", arg));
      }
    }

    return args;
  }

  private boolean hasAnyRequiredArgs(QueryContext context, SQRLTable sqrlTable) {
    //Todo: also check for table functions
    return context.getArgs().stream()
        .filter(a->a.getType() instanceof NonNullType)
        .findAny()
        .isPresent();
  }

  private RelPair createRelNode(QueryContext context, SQRLTable sqrlTable) {
    //todo: general case for table functions. No visibility with this relnode resolution strategy.

    return new RelPair(relBuilder.scan(sqrlTable.getVt().getNameId()).build(), new ArrayList());
  }

  @Value
  public class RelPair {
    RelNode relNode;
    List<PgParameterHandler> handlers;
  }

  private boolean isStructurallySubset(List<Field> accessibleFields,
      List<FieldDefinition> fieldDefinitions) {
    //field definition must have all accessible fields
    Set<String> fieldNames = fieldDefinitions.stream().map(f->f.getName().toLowerCase(Locale.ROOT)).collect(Collectors.toSet());
    Set<String> sqrlNames = accessibleFields.stream().map(f->f.getName().getCanonical().toLowerCase(Locale.ROOT))
        .collect(Collectors.toSet());

    return sqrlNames.containsAll(fieldNames);
  }

  private void resolveQuery(UnionSQRLTable sqrlTable, FieldDefinition fieldDefinition,
      ObjectTypeDefinition parent, Optional<Relationship> relationship) {

  }

  private void resolveQuery(RootJoinSQRLTable sqrlTable, FieldDefinition fieldDefinition,
      ObjectTypeDefinition parent, Optional<Relationship> relationship) {

  }

  private TypeDefinition unwrapObjectType(Type type) {
    //type can be in a single array with any non-nulls, e.g. [customer!]!
    type = unboxNonNull(type);
    if (type instanceof ListType) {
      type = ((ListType) type).getType();
    }
    type = unboxNonNull(type);

    Optional<TypeDefinition> typeDef = this.registry.getType(type);

    Preconditions.checkState(typeDef.isPresent(), "Could not find Object type");

    return typeDef.get();
  }

  private boolean isListType(Type type) {
    return unboxNonNull(type) instanceof ListType;
  }

  private Type unboxNonNull(Type type) {
    if (type instanceof NonNullType) {
      return unboxNonNull(((NonNullType)type).getType());
    }
    return type;
  }

  private QueryContext createQueryContext(SQRLTable sqrlTable, FieldDefinition fieldDefinition,
      ObjectTypeDefinition parent, Multiplicity multiplicity, List<InputValueDefinition> args) {
    return new QueryContext(sqrlTable, fieldDefinition, parent, multiplicity, args);
  }

  private void resolveMutations(ObjectTypeDefinition m) {

  }

  private void resolveSubscriptions(ObjectTypeDefinition s) {

  }
}
