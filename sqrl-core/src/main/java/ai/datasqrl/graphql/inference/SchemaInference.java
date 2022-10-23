package ai.datasqrl.graphql.inference;

import ai.datasqrl.graphql.server.Model.*;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.plan.calcite.Planner;
import ai.datasqrl.plan.calcite.PlannerFactory;
import ai.datasqrl.plan.calcite.TranspilerFactory;
import ai.datasqrl.plan.calcite.table.VirtualRelationalTable;
import ai.datasqrl.plan.local.transpile.ReplaceWithVirtualTable.ExtractRightDeepAlias;
import ai.datasqrl.plan.local.generate.Resolve.Env;
import ai.datasqrl.plan.queries.APIQuery;
import ai.datasqrl.schema.Relationship;
import ai.datasqrl.schema.Relationship.JoinType;
import ai.datasqrl.schema.SQRLTable;
import ai.datasqrl.schema.builder.VirtualTable;
import graphql.language.FieldDefinition;
import graphql.language.InputValueDefinition;
import graphql.language.ObjectTypeDefinition;
import graphql.language.TypeDefinition;
import graphql.schema.idl.SchemaParser;
import graphql.schema.idl.TypeDefinitionRegistry;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.tools.RelBuilder;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
public class SchemaInference {

  private final Planner planner;
  @Getter
  List<APIQuery> apiQueries = new ArrayList<>();

  public SchemaInference(Planner planner) {

    this.planner = planner;
  }

  public Root visitSchema(String schemaStr, Env env) {
    Root.RootBuilder root = Root.builder().schema(StringSchema.builder().schema(schemaStr).build());

    TypeDefinitionRegistry typeDefinitionRegistry = (new SchemaParser()).parse(schemaStr);

    return visit(root, typeDefinitionRegistry, env);
  }

  public Root visitTypeDefinitionRegistry(TypeDefinitionRegistry registry, Env env) {
    Root.RootBuilder root = Root.builder()
        .schema(TypeDefinitionSchema.builder().typeDefinitionRegistry(registry).build());
    return visit(root, registry, env);
  }

  @Value
  class Entry {

    ObjectTypeDefinition objectTypeDefinition;
    Optional<SQRLTable> table;
    VirtualRelationalTable parentVt;
    RelNode parentRelNode;
  }

  private Root visit(Root.RootBuilder root, TypeDefinitionRegistry registry, Env env) {
    Set<String> seenNodes = new HashSet<>();
    Queue<Entry> horizon = new ArrayDeque<>();
    Map<ObjectTypeDefinition, SQRLTable> associatedTypes = new HashMap<>();

    //1. Queue all Query fields
    ObjectTypeDefinition objectTypeDefinition = (ObjectTypeDefinition) registry.getType("Query")
        .get();
    horizon.add(new Entry(objectTypeDefinition, Optional.empty(), null, null));

    List<ArgumentHandler> argumentHandlers = List.of(new EqHandler());

    while (!horizon.isEmpty()) {
      Entry type = horizon.poll();
      seenNodes.add(type.getObjectTypeDefinition().getName());

      for (FieldDefinition field : type.getObjectTypeDefinition().getFieldDefinitions()) {
        if (!(registry.getType(field.getType()).get() instanceof ObjectTypeDefinition)) {
          //scalar?
          continue;
        }
        Optional<Relationship> rel = type.getTable()
            .map(t -> ((Relationship) t.getField(Name.system(field.getName())).get()));

        SQRLTable table = rel.map(Relationship::getToTable).orElseGet(
            () -> (SQRLTable) env.getRelSchema().getTable(field.getName(), false).getTable());
        //todo check if we've already registered the type
        VirtualRelationalTable vt = table.getVt();

        RelNode relNode = constructRel(table, (VirtualRelationalTable) vt, rel, env);
        Set<RelAndArg> args = new HashSet<>();

        //Todo: if all args are optional (just assume there is a no-arg for now)
        args.add(new RelAndArg(relNode, new LinkedHashSet<>(), null, null));

        //todo: don't use arg index size, some args are fixed
        for (InputValueDefinition arg : field.getInputValueDefinitions()) {
          boolean handled = false;
          for (ArgumentHandler handler : argumentHandlers) {
            ArgumentHandlerContextV1 contextV1 = new ArgumentHandlerContextV1(arg, args,
                type.getObjectTypeDefinition(), field, table, vt,
                env.getSession().getPlanner().getRelBuilder());
            if (handler.canHandle(contextV1)) {
              args = handler.accept(contextV1);
              handled = true;
              break;
            }
          }
          if (!handled) {
            log.error("Unhandled Arg : {}", arg);
          }
        }

        ArgumentLookupCoords.ArgumentLookupCoordsBuilder coordsBuilder = ArgumentLookupCoords.builder()
            .parentType(type.getObjectTypeDefinition().getName()).fieldName(field.getName());

        for (RelAndArg arg : args) {
          int keysize =
              rel.isPresent() ? rel.get().getFromTable().getVt().getNumPrimaryKeys()
                  : table.getVt().getNumPrimaryKeys();
          relNode = arg.relNode;
          for (int i = 0; i < keysize; i++) { //todo align with the argument building
            relNode = addPkNode(env, arg.getArgumentSet().size() + i, i, relNode, rel);
          }

          List<PgParameterHandler> argHandler = arg.getArgumentSet().stream()
              .map(a -> ArgumentPgParameter.builder().path(a.getPath()).build())
              .collect(Collectors.toList());

          List<SourcePgParameter> params = addContextToArg(arg, rel, type,
              (VirtualRelationalTable) vt, keysize);
          argHandler.addAll(params);

//          relNode = optimize2(env, relNode);
          APIQuery query = new APIQuery(UUID.randomUUID().toString(), relNode);
          apiQueries.add(query);
          coordsBuilder.match(ArgumentSet.builder().arguments(arg.getArgumentSet()).query(
              ApiQueryBase.builder().query(query).relNode(relNode).relAndArg(arg).parameters(argHandler)
                  .build()).build()).build();
        }

        root.coord(coordsBuilder.build());

        TypeDefinition def = registry.getType(field.getType()).get();
        if (def instanceof ObjectTypeDefinition && !seenNodes.contains(def.getName())) {
          ObjectTypeDefinition resultType = (ObjectTypeDefinition) def;
          horizon.add(
              new Entry(resultType, Optional.of(table), (VirtualRelationalTable) vt, relNode));
        }
      }
    }

    return root.build();
  }

  private RelNode addPkNode(Env env, int index,
      int i, RelNode relNode, Optional<Relationship> rel) {
    if (rel.isEmpty()) {
      return relNode;
    }
    RelBuilder builder = env.getSession().getPlanner().getRelBuilder();
    RexBuilder rex = builder.getRexBuilder();

    RexDynamicParam param = builder.getRexBuilder().makeDynamicParam(relNode.getRowType(), index);
    RelNode newRelNode = builder.push(relNode)
        .filter(rex.makeCall(SqlStdOperatorTable.EQUALS, rex.makeInputRef(relNode, i), param))
        .build();
    return newRelNode;
  }

  private List<SourcePgParameter> addContextToArg(RelAndArg arg, Optional<Relationship> rel,
      Entry type, VirtualRelationalTable vt, int keys) {
    if (rel.isPresent()) {

      return IntStream.range(0, keys).mapToObj(i -> rel.get().getFromTable().getVt().getPrimaryKeyNames().get(i))
          .map(f -> new SourcePgParameter(f)).collect(Collectors.toList());
    }
    return List.of();
  }

  @Value
  public static class RelAndArg {

    RelNode relNode;
    Set<Argument> argumentSet;
    String name;
    RexDynamicParam dynamicParam;
  }

  private RelNode constructRel(SQRLTable table, VirtualRelationalTable vt,
      Optional<Relationship> rel, Env env) {
    if (rel.isPresent() && rel.get().getJoinType() == JoinType.JOIN) {
      return constructJoinDecScan(env, rel.get());
    }

    SqlValidator sqrlValidator = TranspilerFactory.createSqrlValidator(env.getRelSchema(),
        List.of(), true);
    RelBuilder relBuilder = PlannerFactory.sqlToRelConverterConfig.getRelBuilderFactory()
        .create(env.getSession().getPlanner().getCluster(),
            (CalciteCatalogReader) sqrlValidator.getCatalogReader());
    return relBuilder.scan(vt.getNameId()).build();
  }

  private RelNode constructJoinDecScan(Env env, Relationship rel) {
    SqlValidator sqrlValidator = TranspilerFactory.createSqrlValidator(env.getRelSchema(),
        List.of(), true);

    //todo: fix for TOP N
    ExtractRightDeepAlias rightDeepAlias = new ExtractRightDeepAlias();
    String alias = rel.getNode().accept(rightDeepAlias);

    SqlSelect select = new SqlSelect(
        SqlParserPos.ZERO,
        null,
        new SqlNodeList(List.of(SqlIdentifier.star(SqlParserPos.ZERO)), SqlParserPos.ZERO),
        rel.getNode(),
        null,
        null,
        null
        ,
        SqlNodeList.EMPTY,
        null,null,null, SqlNodeList.EMPTY
    );

    SqlNode validated = sqrlValidator.validate(select);
    env.getSession().getPlanner().setValidator(validated, sqrlValidator);
    RelNode relNode = env.getSession().getPlanner().rel(validated).rel;

    return relNode;
  }

  interface ArgumentHandler {

    //Eventually public api
    public Set<RelAndArg> accept(ArgumentHandlerContextV1 context);

    boolean canHandle(ArgumentHandlerContextV1 contextV1);
  }

  @Value
  class ArgumentHandlerContextV1 {

    InputValueDefinition arg;
    Set<RelAndArg> relAndArgs;
    ObjectTypeDefinition type;
    FieldDefinition field;
    SQRLTable table;
    VirtualTable virtualTable;
    RelBuilder relBuilder;
  }

  public static class EqHandler implements ArgumentHandler {

    @Override
    public Set<RelAndArg> accept(ArgumentHandlerContextV1 context) {
      //if optional, assure we re-emit all args
      Set<RelAndArg> set = new HashSet<>(context.getRelAndArgs());
      RexBuilder rexBuilder = context.getRelBuilder().getRexBuilder();
      for (RelAndArg args : context.getRelAndArgs()) {
        RelBuilder relBuilder = context.getRelBuilder();
        relBuilder.push(args.relNode);

        RelDataTypeField field = relBuilder.peek().getRowType()
            .getField(context.arg.getName(), false, false);
        RexDynamicParam dynamicParam = rexBuilder.makeDynamicParam(field.getType(),
            args.argumentSet.size());
        RelNode rel = relBuilder.filter(rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(relBuilder.peek(), field.getIndex()), dynamicParam)).build();

        Set<Argument> newArgs = new LinkedHashSet<>(args.argumentSet);
        newArgs.add(VariableArgument.builder().path(context.arg.getName()).build());

        set.add(new RelAndArg(rel, newArgs, field.getName(), dynamicParam));
      }

      //if optional: add an option to the arg permutation list
      return set;
    }

    @Override
    public boolean canHandle(ArgumentHandlerContextV1 contextV1) {
      return true;
    }
  }

  @Builder
  @Getter
  @AllArgsConstructor
  @NoArgsConstructor
  public static class ApiQueryBase implements QueryBase {
    final String type = "pgQuery";
    APIQuery query;
    RelNode relNode;
    RelAndArg relAndArg;
    @Singular
    List<PgParameterHandler> parameters;

    @Override
    public <R, C> R accept(QueryBaseVisitor<R, C> visitor, C context) {
      ApiQueryVisitor<R, C> visitor1 = (ApiQueryVisitor<R, C>) visitor;
      return visitor1.visitApiQuery(this, context);
    }
  }
  public interface ApiQueryVisitor<R,C> extends QueryBaseVisitor<R, C> {
    R visitApiQuery(ApiQueryBase apiQueryBase, C context);
  }
}
