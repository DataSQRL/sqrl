package ai.datasqrl.graphql.inference;

import ai.datasqrl.graphql.server.Model.*;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.plan.calcite.OptimizationStage;
import ai.datasqrl.plan.calcite.Planner;
import ai.datasqrl.plan.calcite.PlannerFactory;
import ai.datasqrl.plan.calcite.TranspilerFactory;
import ai.datasqrl.plan.calcite.rules.AnnotatedLP;
import ai.datasqrl.plan.calcite.rules.SQRLLogicalPlanConverter;
import ai.datasqrl.plan.calcite.table.VirtualRelationalTable;
import ai.datasqrl.plan.calcite.util.RelToSql;
import ai.datasqrl.plan.local.generate.Resolve.Env;
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
import lombok.Getter;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriterConfig;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.validate.SqrlValidatorImpl;
import org.apache.calcite.tools.RelBuilder;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
public class SchemaInference {

  private final Planner planner;

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
            () -> (SQRLTable) env.getUserSchema().getTable(field.getName(), false).getTable());
        //todo check if we've already registered the type
        VirtualTable vt = env.getTableMap().get(table);

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
              rel.isPresent() ? Math.min(rel.get().getFromTable().getVt().getNumPrimaryKeys(),
                  rel.get().getToTable().getVt().getNumPrimaryKeys())
                  : table.getVt().getNumPrimaryKeys();
          for (int i = 0; i < keysize; i++) { //todo align with the argument building
            relNode = addPkNode(env, arg.getArgumentSet().size() + i, i, arg.relNode, rel);
          }

          List<PgParameterHandler> argHandler = arg.getArgumentSet().stream()
              .map(a -> ArgumentPgParameter.builder().path(a.getPath()).build())
              .collect(Collectors.toList());

          List<SourcePgParameter> params = addContextToArg(arg, rel, type,
              (VirtualRelationalTable) vt);
          argHandler.addAll(params);

          coordsBuilder.match(ArgumentSet.builder().arguments(arg.getArgumentSet()).query(
              PgQuery.builder().sql(convertDynamicParams(relNode, arg)).parameters(argHandler)
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

  private @NonNull RelNode optimize(Env env, RelNode relNode) {
    List<String> fieldNames = relNode.getRowType().getFieldNames();

    relNode = env.getSession().getPlanner()
        .transform(OptimizationStage.PUSH_FILTER_INTO_JOIN, relNode);
//    System.out.println("LP$1: \n" + relNode.explain());

    //Step 2: Convert all special SQRL conventions into vanilla SQL and remove
    //self-joins (including nested self-joins) as well as infer primary keys,
    //table types, and timestamps in the process

    //TODO: extract materialization preference from hints if present
    SQRLLogicalPlanConverter sqrl2sql = new SQRLLogicalPlanConverter(
        () -> env.getSession().getPlanner().getRelBuilder(), Optional.empty());
    relNode = relNode.accept(sqrl2sql);
//    System.out.println("LP$2: \n" + relNode.explain());
    AnnotatedLP prel = sqrl2sql.getRelHolder(relNode);
    prel = prel.postProcess(sqrl2sql.makeRelBuilder(), fieldNames);

    return prel.getRelNode();
  }

  private String convertDynamicParams(RelNode relNode, RelAndArg arg) {
    SqlNode node = RelToSql.convertToSqlNode(relNode);

    UnaryOperator<SqlWriterConfig> transform = c -> c.withAlwaysUseParentheses(false)
        .withSelectListItemsOnSeparateLines(false).withUpdateSetListNewline(false)
        .withIndentation(1).withDialect(PostgresqlSqlDialect.DEFAULT).withSelectFolding(null);

    SqlWriterConfig config = transform.apply(SqlPrettyWriter.config());
    DynamicParamSqlPrettyWriter writer = new DynamicParamSqlPrettyWriter(config, arg);
    node.unparse(writer, 0, 0);

    return writer.toSqlString().getSql();
  }

  /**
   * Writes postgres style dynamic params `$1` instead of `?`. Assumes the index field is the index
   * of the parameter.
   */
  public class DynamicParamSqlPrettyWriter extends SqlPrettyWriter {

    @Getter
    private List<Integer> dynamicParameters = new ArrayList<>();

    public DynamicParamSqlPrettyWriter(@NotNull SqlWriterConfig config, RelAndArg arg) {
      super(config);
    }

    //Write the current index but emit the arg index that it maps to
    int i = 0;

    @Override
    public void dynamicParam(int index) {
      if (dynamicParameters == null) {
        dynamicParameters = new ArrayList<>();
      }
      dynamicParameters.add(index);
      print("$" + (index + 1));
      setNeedWhitespace(true);
    }
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
        .filter(rex.makeCall(SqlStdOperatorTable.EQUALS, rex.makeInputRef(relNode, 0), param))
        .build();
    return newRelNode;
  }

  private List<SourcePgParameter> addContextToArg(RelAndArg arg, Optional<Relationship> rel,
      Entry type, VirtualRelationalTable vt) {
    if (rel.isPresent()) {
      int keys = Math.min(type.getParentVt().getPrimaryKeyNames().size(),
          vt.getPrimaryKeyNames().size());

      return IntStream.range(0, keys).mapToObj(i -> type.getParentVt().getPrimaryKeyNames().get(i))
          .map(f -> new SourcePgParameter(f)).collect(Collectors.toList());
    }
    return List.of();
  }

  @Value
  static class RelAndArg {

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

    SqrlValidatorImpl sqrlValidator = TranspilerFactory.createSqrlValidator(env.getRelSchema(),
        List.of(), true);
    RelBuilder relBuilder = PlannerFactory.sqlToRelConverterConfig.getRelBuilderFactory()
        .create(env.getSession().getPlanner().getCluster(),
            (CalciteCatalogReader) sqrlValidator.getCatalogReader());
    return relBuilder.scan(vt.getNameId()).build();
  }

  private RelNode constructJoinDecScan(Env env, Relationship rel) {
    SqrlValidatorImpl sqrlValidator = TranspilerFactory.createSqrlValidator(env.getRelSchema(),
        List.of(), true);
    SqlNode validated = sqrlValidator.validate(rel.getNode());
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
}
