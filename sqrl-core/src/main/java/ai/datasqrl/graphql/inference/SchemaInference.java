package ai.datasqrl.graphql.inference;

import ai.datasqrl.graphql.server.Model.Argument;
import ai.datasqrl.graphql.server.Model.ArgumentLookupCoords;
import ai.datasqrl.graphql.server.Model.ArgumentSet;
import ai.datasqrl.graphql.server.Model.PgQuery;
import ai.datasqrl.graphql.server.Model.Root;
import ai.datasqrl.graphql.server.Model.TypeDefinitionSchema;
import ai.datasqrl.plan.calcite.PlannerFactory;
import ai.datasqrl.plan.calcite.TranspilerFactory;
import ai.datasqrl.plan.calcite.util.RelToSql;
import ai.datasqrl.plan.local.generate.Resolve.Env;
import ai.datasqrl.schema.SQRLTable;
import ai.datasqrl.schema.builder.VirtualTable;
import graphql.language.FieldDefinition;
import graphql.language.InputValueDefinition;
import graphql.language.Node;
import graphql.language.ObjectTypeDefinition;
import graphql.language.TypeDefinition;
import graphql.schema.GraphQLObjectType;
import graphql.schema.idl.TypeDefinitionRegistry;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
import org.apache.calcite.sql.validate.SqrlValidatorImpl;
import org.apache.calcite.tools.RelBuilder;

@Slf4j
public class SchemaInference {
  public Root visitTypeDefinitionRegistry(TypeDefinitionRegistry registry, Env env) {
    Root.RootBuilder root = Root.builder()
        .schema(TypeDefinitionSchema.builder().typeDefinitionRegistry(registry).build());

    Set<Node> seenNodes = new HashSet<>();
    Queue<ObjectTypeDefinition> horizon = new ArrayDeque<>();
    Map<ObjectTypeDefinition, SQRLTable> associatedTypes = new HashMap<>();

    //1. Queue all Query fields
    ObjectTypeDefinition objectTypeDefinition = (ObjectTypeDefinition) registry.getType("Query")
        .get();
    horizon.add(objectTypeDefinition);

    List<ArgumentHandler> argumentHandlers = new ArrayList();

    while (!horizon.isEmpty()) {
      ObjectTypeDefinition type = horizon.poll();
      seenNodes.add(type);

      for (FieldDefinition field : type.getFieldDefinitions()) {
        SQRLTable table = (SQRLTable) env.getUserSchema().getTable(field.getName(), false)
            .getTable();
        //todo check if we've already registered the type
        VirtualTable vt = env.getTableMap().get(table);

        RelNode relNode = constructTableScan(table, vt, env);
        Set<RelAndArg> args = new HashSet<>();

        //Todo: if all args are optional (just assume there is a no-arg for now)
        args.add(new RelAndArg(relNode, new HashSet<>()));
        //assume each arg does exactly one thing, we can optimize it later

        for (InputValueDefinition arg : field.getInputValueDefinitions()) {
          for (ArgumentHandler handler : argumentHandlers) {
            ArgumentHandlerContextV1 contextV1 = new ArgumentHandlerContextV1(arg, args, type, field,
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
              .parentType(type.getName())
              .fieldName(field.getName());

        //If context, add context, sqrl could have params too

        for (RelAndArg arg : args) {
          coordsBuilder.match(ArgumentSet.builder()
              .arguments(arg.getArgumentSet())
              .query(PgQuery.builder()
                  .sql(RelToSql.convertToSql(arg.relNode, PostgresqlSqlDialect.DEFAULT))
//                  .parameter(ArgumentPgParameter.builder()
//                      .path("customerid")
                      .build()
                  )
                  .build())
              .build();
        }

        root.coord(coordsBuilder.build());

        TypeDefinition def = registry.getType(field.getType()).get();
        if (def instanceof GraphQLObjectType &&
            !seenNodes.contains(def)) {
          ObjectTypeDefinition resultType = (ObjectTypeDefinition) def;
          horizon.add(resultType);
        }
      }
    }

    return root.build();
  }

  @Value
  static class RelAndArg {

    RelNode relNode;
    Set<Argument> argumentSet;
  }

  private RelNode constructTableScan(SQRLTable table, VirtualTable vt, Env env) {
    SqrlValidatorImpl sqrlValidator = TranspilerFactory.createSqrlValidator(env.getUserSchema());
    RelBuilder relBuilder = PlannerFactory.sqlToRelConverterConfig.getRelBuilderFactory()
        .create(env.getSession().getPlanner().getCluster(),
            (CalciteCatalogReader)sqrlValidator.getCatalogReader()
            );
    return relBuilder
        .scan(table.getName().getCanonical().toLowerCase(Locale.ROOT))
        .build();
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
