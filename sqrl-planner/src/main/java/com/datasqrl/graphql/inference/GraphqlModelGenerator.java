package com.datasqrl.graphql.inference;

import static com.datasqrl.graphql.generate.GraphqlSchemaUtil.hasVaryingCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.calcite.jdbc.SqrlSchema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datasqrl.calcite.Dialect;
import com.datasqrl.calcite.QueryPlanner;
import com.datasqrl.calcite.function.SqrlTableMacro;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.engine.PhysicalPlan;
import com.datasqrl.engine.PhysicalPlan.PhysicalStagePlan;
import com.datasqrl.engine.database.QueryTemplate;
import com.datasqrl.engine.log.kafka.KafkaPhysicalPlan;
import com.datasqrl.engine.log.postgres.PostgresLogPhysicalPlan;
import com.datasqrl.graphql.APIConnectorManager;
import com.datasqrl.graphql.jdbc.DatabaseType;
import com.datasqrl.graphql.server.RootGraphqlModel.ArgumentLookupQueryCoords;
import com.datasqrl.graphql.server.RootGraphqlModel.FieldLookupQueryCoords;
import com.datasqrl.graphql.server.RootGraphqlModel.KafkaMutationCoords;
import com.datasqrl.graphql.server.RootGraphqlModel.KafkaSubscriptionCoords;
import com.datasqrl.graphql.server.RootGraphqlModel.MutationCoords;
import com.datasqrl.graphql.server.RootGraphqlModel.PostgresLogMutationCoords;
import com.datasqrl.graphql.server.RootGraphqlModel.PostgresSubscriptionCoords;
import com.datasqrl.graphql.server.RootGraphqlModel.QueryCoords;
import com.datasqrl.graphql.server.RootGraphqlModel.QueryWithArguments;
import com.datasqrl.graphql.server.RootGraphqlModel.SqlQuery;
import com.datasqrl.graphql.server.RootGraphqlModel.SubscriptionCoords;
import com.datasqrl.plan.queries.APIQuery;
import com.datasqrl.plan.queries.APISource;
import com.datasqrl.plan.queries.APISubscription;
import com.datasqrl.plan.queries.IdentifiedQuery;
import com.google.common.base.Preconditions;

import graphql.language.FieldDefinition;
import graphql.language.InputValueDefinition;
import graphql.language.ObjectTypeDefinition;
import graphql.schema.idl.TypeDefinitionRegistry;
import lombok.Getter;

/**
 * Returns a set of table functions that satisfy a graphql schema
 */
@Getter
public class GraphqlModelGenerator extends SchemaWalker {

  private static final Logger log = LoggerFactory.getLogger(GraphqlModelGenerator.class);
  private final PhysicalPlan physicalPlan;
  private final QueryPlanner queryPlanner;
  List<QueryCoords> coords = new ArrayList<>();
  List<MutationCoords> mutations = new ArrayList<>();
  List<SubscriptionCoords> subscriptions = new ArrayList<>();

  public GraphqlModelGenerator(SqlNameMatcher nameMatcher, SqrlSchema schema,
      Map<IdentifiedQuery, QueryTemplate> databaseQueries, QueryPlanner queryPlanner,
      APIConnectorManager apiManager, PhysicalPlan physicalPlan) {
    super(nameMatcher, schema, apiManager);
    this.physicalPlan = physicalPlan;
    this.queryPlanner = queryPlanner;
  }

  @Override
  protected void walkSubscription(ObjectTypeDefinition m, FieldDefinition fieldDefinition,
      TypeDefinitionRegistry registry, APISource source) {
    var apiSubscription = new APISubscription(Name.system(fieldDefinition.getName()),
        source);
    var tableFunction = schema.getTableFunction(fieldDefinition.getName());
    if (tableFunction == null) {
      throw new RuntimeException("Table '" + fieldDefinition.getName() + "' does not exist in the schema.");
    }
    var log = apiManager.addSubscription(apiSubscription, tableFunction);

    Map<String, String> filters = new HashMap<>();
    for (InputValueDefinition input : fieldDefinition.getInputValueDefinitions()) {
      RelDataTypeField field = nameMatcher.field(tableFunction.getRowType(), input.getName());
      if (field == null) {
        throw new RuntimeException("Field '" + input.getName() + "' does not exist in subscription '" + fieldDefinition.getName() + "'.");
      }
      filters.put(input.getName(), field.getName());
    }

    var logPlan = getLogPlan();

    var fieldName = fieldDefinition.getName();

    SubscriptionCoords subscriptionCoords;

    if (logPlan.isPresent() && logPlan.get() instanceof KafkaPhysicalPlan) {
      var topic = (String) log.getConnectorContext().getMap().get("topic");
      subscriptionCoords = new KafkaSubscriptionCoords(fieldName, topic, Map.of(), filters);
    } else if (logPlan.isPresent() && logPlan.get() instanceof PostgresLogPhysicalPlan) {
      var tableName = (String) log.getConnectorContext().getMap().get("table-name");

      var listenNotifyAssets = ((PostgresLogPhysicalPlan) logPlan.get())
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
  }


  @Override
  protected void walkMutation(APISource source, TypeDefinitionRegistry registry,
      ObjectTypeDefinition m, FieldDefinition fieldDefinition) {
    var tableSource = apiManager.getMutationSource(source,
        Name.system(fieldDefinition.getName()));

    var src = schema.getImports().stream()
        .filter(t -> t.getName().equalsIgnoreCase(fieldDefinition.getName()))
        .map(t -> t.getTableConfig())
        .findFirst();

    var logPlan = getLogPlan();

    MutationCoords mutationCoords;

    if (logPlan.isPresent() && logPlan.get() instanceof KafkaPhysicalPlan) {
      String topicName;
      if (tableSource != null) {
        var map = tableSource.getConfiguration().getConnectorConfig().toMap();
        topicName = (String) map.get("topic");
      } else if (src.isPresent()) {
        var map = src.get().getConnectorConfig().toMap();
        topicName = (String) map.get("topic");
      } else {
        throw new RuntimeException("Could not find mutation: " + fieldDefinition.getName());
      }
      if (topicName == null) {
        throw new RuntimeException("Missing 'topic' configuration for mutation '" + fieldDefinition.getName() + "'.");
      }

      mutationCoords = new KafkaMutationCoords(fieldDefinition.getName(), topicName, Map.of(), Map.of());
    } else if (logPlan.isPresent() && logPlan.get() instanceof PostgresLogPhysicalPlan) {
      String tableName;
      if (tableSource != null) {
        var map = tableSource.getConfiguration().getConnectorConfig().toMap();
        tableName = (String) map.get("table-name");
      } else if (src.isPresent()) {
        // TODO: not sure if this is correct and needed
        var map = src.get().getConnectorConfig().toMap();
        tableName = (String) map.get("table-name");
      } else {
        throw new RuntimeException("Could not find mutation: " + fieldDefinition.getName());
      }

      var insertStatement = ((PostgresLogPhysicalPlan) logPlan.get())
          .getInserts().stream()
          .filter(insert -> insert.getTableName().equals(tableName))
          .findFirst()
          .orElseThrow(
              () -> new RuntimeException("Could not find insert statement for table: " + tableName)
          );

      mutationCoords = new PostgresLogMutationCoords(fieldDefinition.getName(), tableName,
          insertStatement.getSql(), insertStatement.getParams());
    } else {
      throw new RuntimeException("Unknown log plan: " + logPlan.getClass().getName());
    }

    mutations.add(mutationCoords);
  }

  private Optional<EnginePhysicalPlan> getLogPlan() {
    //Todo: Bad instance checking, Fix to bring multiple log engines (?)
    var logPlan = physicalPlan.getStagePlans().stream()
        .map(PhysicalStagePlan::getPlan)
        .filter(plan -> plan instanceof KafkaPhysicalPlan || plan instanceof PostgresLogPhysicalPlan)
        .findFirst();
    return logPlan;
  }

  @Override
  protected void visitUnknownObject(ObjectTypeDefinition type, FieldDefinition field, NamePath path,
      Optional<RelDataType> rel) {
  }

  @Override
  protected void visitScalar(ObjectTypeDefinition type, FieldDefinition field, NamePath path,
      RelDataType relDataType, RelDataTypeField relDataTypeField) {
    //todo: walk into structured type to check all prop fetchers

    if (hasVaryingCase(field, relDataTypeField)) {
      var build = FieldLookupQueryCoords.builder().parentType(type.getName())
          .fieldName(field.getName()).columnName(relDataTypeField.getName()).build();
      coords.add(build);
    }

  }

  @Override
  protected void visitQuery(ObjectTypeDefinition parentType, ObjectTypeDefinition toType,
      FieldDefinition field, NamePath path, Optional<RelDataType> parentRel,
      List<SqrlTableMacro> functions) {

    List<Entry<IdentifiedQuery, QueryTemplate>> queries = physicalPlan.getDatabaseQueries()
        .entrySet().stream()
        .filter(f -> ((APIQuery) f.getKey()).getNamePath().equals(path))
        .collect(Collectors.toList());

    // No queries: use a property fetcher
    if (queries.isEmpty()) {
      return;
    }
    Preconditions.checkArgument(queries.size()==1, "Path %s should map to a single query but found: %s", path, queries);

    var coordsBuilder = ArgumentLookupQueryCoords.builder()
        .parentType(parentType.getName()).fieldName(field.getName());

    for (Entry<IdentifiedQuery, QueryTemplate> entry : queries) {
      SqlQuery queryBase = null;
      var query = (APIQuery) entry.getKey();

      String queryStr;
      if (entry.getValue().getDatabase()== DatabaseType.SNOWFLAKE) {
        queryStr = QueryPlanner.relToString(Dialect.SNOWFLAKE,
                queryPlanner.convertRelToDialect(Dialect.SNOWFLAKE, entry.getValue().getRelNode()))
            .getSql();
      } else {
        queryStr = QueryPlanner.relToString(Dialect.POSTGRES,
                queryPlanner.convertRelToDialect(Dialect.POSTGRES, entry.getValue().getRelNode()))
            .getSql();
      }

      var set = QueryWithArguments.builder().arguments(query.getGraphqlArguments())
          .query(queryBase).build();

      coordsBuilder.exec(set);
    }

    var coord = coordsBuilder.build();
    coords.add(coord);
  }
}
