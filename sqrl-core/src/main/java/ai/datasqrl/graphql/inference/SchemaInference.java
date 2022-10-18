package ai.datasqrl.graphql.inference;

import ai.datasqrl.graphql.server.Model.Argument;
import ai.datasqrl.graphql.server.Model.ArgumentLookupCoords;
import ai.datasqrl.graphql.server.Model.ArgumentSet;
import ai.datasqrl.graphql.server.Model.PgParameterHandler;
import ai.datasqrl.graphql.server.Model.PgQuery;
import ai.datasqrl.graphql.server.Model.Root;
import ai.datasqrl.graphql.server.Model.SourcePgParameter;
import ai.datasqrl.graphql.server.Model.StringSchema;
import ai.datasqrl.graphql.server.Model.TypeDefinitionSchema;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.plan.calcite.PlannerFactory;
import ai.datasqrl.plan.calcite.TranspilerFactory;
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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.validate.SqrlValidatorImpl;
import org.apache.calcite.tools.RelBuilder;

@Slf4j
public class SchemaInference {

  public Root visitSchema(String schemaStr, Env env) {
    Root.RootBuilder root = Root.builder()
        .schema(StringSchema.builder().schema(schemaStr).build());

    TypeDefinitionRegistry typeDefinitionRegistry =
        (new SchemaParser()).parse(schemaStr);

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
  }

  private Root visit(Root.RootBuilder root, TypeDefinitionRegistry registry, Env env) {
    Set<String> seenNodes = new HashSet<>();
    Queue<Entry> horizon = new ArrayDeque<>();
    Map<ObjectTypeDefinition, SQRLTable> associatedTypes = new HashMap<>();

    //1. Queue all Query fields
    ObjectTypeDefinition objectTypeDefinition = (ObjectTypeDefinition) registry.getType("Query")
        .get();
    horizon.add(new Entry(objectTypeDefinition, Optional.empty()));

    List<ArgumentHandler> argumentHandlers = new ArrayList();

    while (!horizon.isEmpty()) {
      Entry type = horizon.poll();
      seenNodes.add(type.getObjectTypeDefinition().getName());

      for (FieldDefinition field : type.getObjectTypeDefinition().getFieldDefinitions()) {
        if (!(registry.getType(field.getType()).get() instanceof ObjectTypeDefinition)) {
          //scalar?
          continue;
        }
        Optional<Relationship> rel = type.getTable()
            .map(t -> (
                (Relationship) t.getField(Name.system(field.getName())).get()));

        SQRLTable table = rel.map(Relationship::getToTable)
            .orElseGet(() ->
                (SQRLTable) env.getUserSchema().getTable(field.getName(), false)
                    .getTable());
        //todo check if we've already registered the type
        VirtualTable vt = env.getTableMap().get(table);

        RelNode relNode = constructRel(table, (VirtualRelationalTable) vt, rel, env);
        Set<RelAndArg> args = new HashSet<>();

        //Todo: if all args are optional (just assume there is a no-arg for now)
        args.add(new RelAndArg(relNode, new HashSet<>()));
        //assume each arg does exactly one thing, we can optimize it later

        for (InputValueDefinition arg : field.getInputValueDefinitions()) {
          for (ArgumentHandler handler : argumentHandlers) {
            ArgumentHandlerContextV1 contextV1 = new ArgumentHandlerContextV1(arg, args,
                type.getObjectTypeDefinition(), field,
                table, vt, env.getSession().getPlanner().getRelBuilder());
            if (handler.canHandle(contextV1)) {
              args = handler.accept(contextV1);
              break;
            }
          }
          log.error("Unhandled Arg : {}", arg);
        }

        ArgumentLookupCoords.ArgumentLookupCoordsBuilder coordsBuilder =
            ArgumentLookupCoords.builder()
                .parentType(type.getObjectTypeDefinition().getName())
                .fieldName(field.getName());

        //If context, add context, sqrl could have params too

        for (RelAndArg arg : args) {
          RelNode parameters = addParameterizedRelNode(env, arg.relNode, rel);
          List<PgParameterHandler> params = addContextToArg(arg, rel);

          coordsBuilder.match(ArgumentSet.builder()
                  .arguments(arg.getArgumentSet())
                  .query(PgQuery.builder()
                          .sql(RelToSql.convertToSql(parameters)
                              .replace("?", "$1"))
                          .parameters(params)
//                  .parameter(ArgumentPgParameter.builder()
//                      .path("customerid")
                          .build()
                  )
                  .build())
              .build();
        }

        root.coord(coordsBuilder.build());

        TypeDefinition def = registry.getType(field.getType()).get();
        if (def instanceof ObjectTypeDefinition &&
            !seenNodes.contains(def.getName())
        ) {
          ObjectTypeDefinition resultType = (ObjectTypeDefinition) def;
          horizon.add(new Entry(resultType, Optional.of(table)));
        }
      }
    }

    return root.build();
  }

  private RelNode addParameterizedRelNode(Env env, RelNode relNode, Optional<Relationship> rel) {
    if (rel.isEmpty()) {
      return relNode;
    }
    RelBuilder builder = env.getSession().getPlanner().getRelBuilder();
    RexBuilder rex = builder.getRexBuilder();
    return builder.push(relNode)
        .filter(
            rex.makeCall(SqlStdOperatorTable.EQUALS,
                rex.makeInputRef(relNode, 0),
                builder.getRexBuilder().makeDynamicParam(relNode.getRowType(), 0)
            ))
        .build();
  }

  private List<PgParameterHandler> addContextToArg(RelAndArg arg, Optional<Relationship> rel) {
    if (rel.isPresent()) {
      SourcePgParameter sourcePgParameter = new SourcePgParameter(
          arg.relNode.getRowType().getFieldList().get(0).getName());

      return List.of(sourcePgParameter);
    }
    return List.of();
  }

  @Value
  static class RelAndArg {

    RelNode relNode;
    Set<Argument> argumentSet;
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
            (CalciteCatalogReader) sqrlValidator.getCatalogReader()
        );
    return relBuilder
        .scan(vt.getNameId())
        .build();
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
}
