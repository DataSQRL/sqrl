package com.datasqrl.v2.graphql;

import static com.datasqrl.graphql.generate.GraphqlSchemaUtil.hasVaryingCase;

import com.datasqrl.calcite.Dialect;
import com.datasqrl.calcite.function.SqrlTableMacro;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.config.TableConfig;
import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.engine.ExecutableQuery;
import com.datasqrl.engine.PhysicalPlan.PhysicalStagePlan;
import com.datasqrl.engine.database.QueryTemplate;
import com.datasqrl.engine.database.relational.ExecutableJdbcReadQuery;
import com.datasqrl.engine.database.relational.ddl.statements.InsertStatement;
import com.datasqrl.engine.database.relational.ddl.statements.notify.ListenNotifyAssets;
import com.datasqrl.engine.log.Log;
import com.datasqrl.engine.log.kafka.KafkaPhysicalPlan;
import com.datasqrl.engine.log.postgres.PostgresLogPhysicalPlan;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.graphql.APIConnectorManager;
import com.datasqrl.graphql.jdbc.SchemaConstants;
import com.datasqrl.graphql.server.RootGraphqlModel;
import com.datasqrl.graphql.server.RootGraphqlModel.Argument;
import com.datasqrl.graphql.server.RootGraphqlModel.ArgumentLookupCoords;
import com.datasqrl.graphql.server.RootGraphqlModel.ArgumentSet;
import com.datasqrl.graphql.server.RootGraphqlModel.Coords;
import com.datasqrl.graphql.server.RootGraphqlModel.DuckDbQuery;
import com.datasqrl.graphql.server.RootGraphqlModel.FieldLookupCoords;
import com.datasqrl.graphql.server.RootGraphqlModel.JdbcQuery;
import com.datasqrl.graphql.server.RootGraphqlModel.KafkaMutationCoords;
import com.datasqrl.graphql.server.RootGraphqlModel.KafkaSubscriptionCoords;
import com.datasqrl.graphql.server.RootGraphqlModel.MutationCoords;
import com.datasqrl.graphql.server.RootGraphqlModel.PagedDuckDbQuery;
import com.datasqrl.graphql.server.RootGraphqlModel.PagedJdbcQuery;
import com.datasqrl.graphql.server.RootGraphqlModel.PagedSnowflakeDbQuery;
import com.datasqrl.graphql.server.RootGraphqlModel.PostgresLogMutationCoords;
import com.datasqrl.graphql.server.RootGraphqlModel.PostgresSubscriptionCoords;
import com.datasqrl.graphql.server.RootGraphqlModel.SnowflakeDbQuery;
import com.datasqrl.graphql.server.RootGraphqlModel.SubscriptionCoords;
import com.datasqrl.io.tables.TableSource;
import com.datasqrl.plan.queries.APIQuery;
import com.datasqrl.plan.queries.APISource;
import com.datasqrl.plan.queries.APISubscription;
import com.datasqrl.plan.queries.IdentifiedQuery;
import com.datasqrl.v2.tables.SqrlTableFunction;
import com.google.common.base.Preconditions;
import graphql.language.FieldDefinition;
import graphql.language.InputValueDefinition;
import graphql.language.ObjectTypeDefinition;
import graphql.schema.idl.TypeDefinitionRegistry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Getter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;

/**
 * Returns a set of table functions that satisfy a graphql schema
 */
@Getter
public class GraphqlModelGenerator2 extends GraphqlSchemaWalker2 {

  List<Coords> queryCoords = new ArrayList<>();
  List<MutationCoords> mutations = new ArrayList<>();
  List<SubscriptionCoords> subscriptions = new ArrayList<>();
  private final ErrorCollector errorCollector;


  public GraphqlModelGenerator2(List<SqrlTableFunction> tableFunctions, APIConnectorManager apiConnectorManager, ErrorCollector errorCollector) {
    super(tableFunctions, apiConnectorManager);
    this.errorCollector = errorCollector;
  }

  @Override
  protected void visitSubscription(ObjectTypeDefinition objectType, FieldDefinition fieldDefinition,
                                   TypeDefinitionRegistry registry, APISource source) {
    /*
    APISubscription apiSubscription = new APISubscription(Name.system(fieldDefinition.getName()),
        source);
    SqrlTableMacro tableFunction = schema.getTableFunction(fieldDefinition.getName());
    if (tableFunction == null) {
      throw new RuntimeException("Table '" + fieldDefinition.getName() + "' does not exist in the schema.");
    }
    Log log = apiManager.addSubscription(apiSubscription, tableFunction);

    Map<String, String> filters = new HashMap<>();
    for (InputValueDefinition input : fieldDefinition.getInputValueDefinitions()) {
      RelDataTypeField field = nameMatcher.field(tableFunction.getRowType(), input.getName());
      if (field == null) {
        throw new RuntimeException("Field '" + input.getName() + "' does not exist in subscription '" + fieldDefinition.getName() + "'.");
      }
      filters.put(input.getName(), field.getName());
    }

    Optional<EnginePhysicalPlan> logPlan = getLogPlan();

    String fieldName = fieldDefinition.getName();

    SubscriptionCoords subscriptionCoords;

    if (logPlan.isPresent() && logPlan.get() instanceof KafkaPhysicalPlan) {
      String topic = (String) log.getConnectorContext().getMap().get("topic");
      subscriptionCoords = new KafkaSubscriptionCoords(fieldName, topic, Map.of(), filters);
    } else if (logPlan.isPresent() && logPlan.get() instanceof PostgresLogPhysicalPlan) {
      String tableName = (String) log.getConnectorContext().getMap().get("table-name");

      ListenNotifyAssets listenNotifyAssets = ((PostgresLogPhysicalPlan) logPlan.get())
          .getQueries().stream()
          .filter(query -> query.getListen().getTableName().equals(tableName))
          .findFirst()
          .orElseThrow(
              () -> new RuntimeException("Could not find query statement for table: " + tableName)
          );

      subscriptionCoords = new PostgresSubscriptionCoords(
          fieldName, tableName, filters,
          listenNotifyAssets.getListen().getSql(),
          listenNotifyAssets.getOnNotify().getSql(),
          listenNotifyAssets.getParameters());
    } else if (logPlan.isEmpty()) {
      throw new RuntimeException("No log plan found. Ensure that a log plan is configured and available.");
    } else {
      throw new RuntimeException("Unknown log plan type: " + logPlan.get().getClass().getName());
    }

    subscriptions.add(subscriptionCoords);
     */
  }


  @Override
  protected void visitMutation(ObjectTypeDefinition objectType, FieldDefinition field, TypeDefinitionRegistry registry, APISource source) {
    /*TableSource tableSource = apiManager.getMutationSource(source,
        Name.system(field.getName()));

    Optional<TableConfig> src = schema.getImports().stream()
        .filter(t -> t.getName().equalsIgnoreCase(field.getName()))
        .map(t -> t.getTableConfig())
        .findFirst();

    Optional<EnginePhysicalPlan> logPlan = getLogPlan();

    MutationCoords mutationCoords;

    if (logPlan.isPresent() && logPlan.get() instanceof KafkaPhysicalPlan) {
      String topicName;
      if (tableSource != null) {
        Map<String, Object> map = tableSource.getConfiguration().getConnectorConfig().toMap();
        topicName = (String) map.get("topic");
      } else if (src.isPresent()) {
        Map<String, Object> map = src.get().getConnectorConfig().toMap();
        topicName = (String) map.get("topic");
      } else {
        throw new RuntimeException("Could not find mutation: " + field.getName());
      }
      if (topicName == null) {
        throw new RuntimeException("Missing 'topic' configuration for mutation '" + field.getName() + "'.");
      }

      mutationCoords = new KafkaMutationCoords(field.getName(), topicName, Map.of());
    } else if (logPlan.isPresent() && logPlan.get() instanceof PostgresLogPhysicalPlan) {
      String tableName;
      if (tableSource != null) {
        Map<String, Object> map = tableSource.getConfiguration().getConnectorConfig().toMap();
        tableName = (String) map.get("table-name");
      } else if (src.isPresent()) {
        // TODO: not sure if this is correct and needed
        Map<String, Object> map = src.get().getConnectorConfig().toMap();
        tableName = (String) map.get("table-name");
      } else {
        throw new RuntimeException("Could not find mutation: " + field.getName());
      }

      InsertStatement insertStatement = ((PostgresLogPhysicalPlan) logPlan.get())
          .getInserts().stream()
          .filter(insert -> insert.getTableName().equals(tableName))
          .findFirst()
          .orElseThrow(
              () -> new RuntimeException("Could not find insert statement for table: " + tableName)
          );

      mutationCoords = new PostgresLogMutationCoords(field.getName(), tableName,
          insertStatement.getSql(), insertStatement.getParams());
    } else {
      throw new RuntimeException("Unknown log plan: " + logPlan.getClass().getName());
    }

    mutations.add(mutationCoords);
    */
  }

/*
private Optional<EnginePhysicalPlan> getLogPlan() {
    //Todo: Bad instance checking, Fix to bring multiple log engines (?)
    Optional<EnginePhysicalPlan> logPlan = physicalPlan.getStagePlans().stream()
        .map(PhysicalStagePlan::getPlan)
        .filter(plan -> plan instanceof KafkaPhysicalPlan || plan instanceof PostgresLogPhysicalPlan)
        .findFirst();
    return logPlan;
  }
  */

  @Override
  protected void visitUnknownObject(ObjectTypeDefinition objectType, FieldDefinition field, NamePath path,
                                    Optional<RelDataType> relDataType) {
  }

  @Override
  protected void visitScalar(ObjectTypeDefinition objectType, FieldDefinition field, NamePath path,
                             RelDataType relDataType, RelDataTypeField relDataTypeField) {
    //todo: walk into structured type to check all prop fetchers

    if (hasVaryingCase(field, relDataTypeField)) {
      FieldLookupCoords build = FieldLookupCoords.builder().parentType(objectType.getName())
          .fieldName(field.getName()).columnName(relDataTypeField.getName()).build();
      queryCoords.add(build);
    }

  }

  @Override
  protected void visitQuery(ObjectTypeDefinition resultType, FieldDefinition field, SqrlTableFunction tableFunction) {
    // As we no more merge user provided graphQL schema with the inferred schema, we no more need to generate as many queries as the permutations of its arguments.
    // We now have a single executable query linked to the table function and already fully defined
    final ExecutableQuery executableQuery = tableFunction.getExecutableQuery();
    ArgumentLookupCoords.ArgumentLookupCoordsBuilder coordsBuilder = ArgumentLookupCoords.builder()
        .parentType(resultType.getName()).fieldName(field.getName());
    ArgumentSet set = ArgumentSet.builder().arguments(createArguments(field))
            .query(new ExecutableQueryToQueryBaseWrapper(executableQuery)).build();

    coordsBuilder.match(set);

    queryCoords.add(coordsBuilder.build());
  }

  //TODO temporary ths is just for model generation, need to wire up all types of queries (paginated or not, snowflake, jdbc, duckdb)
  private static class ExecutableQueryToQueryBaseWrapper implements RootGraphqlModel.QueryBase {
    String sql = "";
    public ExecutableQueryToQueryBaseWrapper(ExecutableQuery executableQuery) {
      //TODO very bad temporary code
      if (executableQuery instanceof ExecutableJdbcReadQuery) {
        final ExecutableJdbcReadQuery executableJdbcReadQuery = (ExecutableJdbcReadQuery) executableQuery;
        this.sql = executableJdbcReadQuery.getSql();
      }
    }

    @Override
    public <R, C> R accept(RootGraphqlModel.QueryBaseVisitor<R, C> visitor, C context) {
      return null;
    }
  }

  private static Set<Argument> createArguments(FieldDefinition field) {
    // create the arguements as they used to be created in QueryBuilderHelper
    Set<Argument> argumentSet = field.getInputValueDefinitions().stream()
            .filter(input -> !input.getName().equals(SchemaConstants.LIMIT) && !input.getName().equals(SchemaConstants.OFFSET))
            .map(input -> RootGraphqlModel.VariableArgument.builder()
                    .path(input.getName())
                    .value(null)
                    .build())
            .collect(Collectors.toSet());
    argumentSet.addAll(field.getInputValueDefinitions().stream()
            .filter(input -> input.getName().equals(SchemaConstants.LIMIT) || input.getName().equals(SchemaConstants.OFFSET))
            .map(input -> RootGraphqlModel.FixedArgument.builder()
                    .path(input.getName())
                    .value(null)
                    .build())
            .collect(Collectors.toSet()));
    return argumentSet;
  }
}
