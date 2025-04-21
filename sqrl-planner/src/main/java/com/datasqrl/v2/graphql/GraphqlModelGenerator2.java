package com.datasqrl.v2.graphql;

import static com.datasqrl.graphql.generate.GraphqlSchemaUtil.hasVaryingCase;
import static com.datasqrl.graphql.util.GraphqlCheckUtil.checkState;

import com.datasqrl.engine.ExecutableQuery;
import com.datasqrl.engine.database.relational.ExecutableJdbcReadQuery;
import com.datasqrl.engine.log.kafka.KafkaQuery;
import com.datasqrl.engine.log.kafka.NewTopic;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.graphql.jdbc.SchemaConstants;
import com.datasqrl.graphql.server.MutationComputedColumnType;
import com.datasqrl.graphql.server.PaginationType;
import com.datasqrl.graphql.server.RootGraphqlModel;
import com.datasqrl.graphql.server.RootGraphqlModel.Argument;
import com.datasqrl.graphql.server.RootGraphqlModel.ArgumentLookupQueryCoords;
import com.datasqrl.graphql.server.RootGraphqlModel.FieldLookupQueryCoords;
import com.datasqrl.graphql.server.RootGraphqlModel.KafkaMutationCoords;
import com.datasqrl.graphql.server.RootGraphqlModel.KafkaSubscriptionCoords;
import com.datasqrl.graphql.server.RootGraphqlModel.MutationCoords;
import com.datasqrl.graphql.server.RootGraphqlModel.QueryCoords;
import com.datasqrl.graphql.server.RootGraphqlModel.QueryParameterHandler;
import com.datasqrl.graphql.server.RootGraphqlModel.QueryWithArguments;
import com.datasqrl.graphql.server.RootGraphqlModel.SqlQuery;
import com.datasqrl.graphql.server.RootGraphqlModel.SubscriptionCoords;
import com.datasqrl.v2.dag.plan.MutationComputedColumn;
import com.datasqrl.v2.dag.plan.MutationQuery;
import com.datasqrl.v2.parser.AccessModifier;
import com.datasqrl.v2.tables.SqrlFunctionParameter;
import com.datasqrl.v2.tables.SqrlTableFunction;
import com.google.common.base.Preconditions;
import graphql.language.FieldDefinition;
import graphql.language.InputValueDefinition;
import graphql.language.ObjectTypeDefinition;
import graphql.schema.idl.TypeDefinitionRegistry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Getter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.FunctionParameter;

/**
 * Returns a set of table functions that satisfy a graphql schema
 */
@Getter
public class GraphqlModelGenerator2 extends GraphqlSchemaWalker2 {

  List<QueryCoords> queryCoords = new ArrayList<>();
  List<MutationCoords> mutations = new ArrayList<>();
  List<SubscriptionCoords> subscriptions = new ArrayList<>();
  private final ErrorCollector errorCollector;


  public GraphqlModelGenerator2(List<SqrlTableFunction> tableFunctions, List<MutationQuery> mutations, ErrorCollector errorCollector) {
    super(tableFunctions, mutations);
    this.errorCollector = errorCollector;
  }

  @Override
  protected void visitSubscription(FieldDefinition atField, SqrlTableFunction tableFunction, TypeDefinitionRegistry registry) {
    Preconditions.checkArgument(tableFunction.getVisibility().getAccess()== AccessModifier.SUBSCRIPTION);
    final ExecutableQuery executableQuery = tableFunction.getExecutableQuery();

    String fieldName = atField.getName();
    List<InputValueDefinition> inputArguments = atField.getInputValueDefinitions();
    SubscriptionCoords subscriptionCoords;
    if (executableQuery instanceof KafkaQuery kafkaQuery) {
      Map<String, String> filters = kafkaQuery.getFilterColumnNames().entrySet().stream().collect(
          Collectors.toMap(e -> inputArguments.get(e.getValue()).getName(), Entry::getKey));
      subscriptionCoords = new KafkaSubscriptionCoords(fieldName, kafkaQuery.getTopicName(), Map.of(), filters);
//    } else if (executableQuery instanceof PostgresSubscriptionQuery) {
//      ListenNotifyAssets listenNotifyAssets = ((PostgresLogPhysicalPlan) logPlan.get())
//          .getQueries().stream()
//          .filter(query -> query.getListen().getTableName().equals(tableName))
//          .findFirst()
//          .orElseThrow(
//              () -> new RuntimeException("Could not find query statement for table: " + tableName)
//          );
//
//      subscriptionCoords = new PostgresSubscriptionCoords(
//          fieldName, tableName, filters,
//          listenNotifyAssets.getListen().getSql(),
//          listenNotifyAssets.getOnNotify().getSql(),
//          listenNotifyAssets.getParameters());
    } else {
      throw new UnsupportedOperationException("Unsupported subscription query: " + executableQuery);
    }
    subscriptions.add(subscriptionCoords);
  }


  @Override
  protected void visitMutation(FieldDefinition atField, TypeDefinitionRegistry registry, MutationQuery mutation) {
    MutationCoords mutationCoords;
    Map<String, MutationComputedColumnType> computedColumns = mutation.getComputedColumns().stream()
        .collect(Collectors.toMap(MutationComputedColumn::getColumnName, MutationComputedColumn::getType));
    if (mutation.getCreateTopic() instanceof NewTopic) {
      NewTopic newTopic = (NewTopic) mutation.getCreateTopic();
      mutationCoords = new KafkaMutationCoords(atField.getName(), newTopic.getTopicName(), computedColumns, Map.of());
//    } else if (logPlan.isPresent() && logPlan.get() instanceof PostgresLogPhysicalPlan) {
//      String tableName;
//      if (tableSource != null) {
//        Map<String, Object> map = tableSource.getConfiguration().getConnectorConfig().toMap();
//        tableName = (String) map.get("table-name");
//      } else if (src.isPresent()) {
//        // TODO: not sure if this is correct and needed
//        Map<String, Object> map = src.get().getConnectorConfig().toMap();
//        tableName = (String) map.get("table-name");
//      } else {
//        throw new RuntimeException("Could not find mutation: " + field.getName());
//      }
//
//      InsertStatement insertStatement = ((PostgresLogPhysicalPlan) logPlan.get())
//          .getInserts().stream()
//          .filter(insert -> insert.getTableName().equals(tableName))
//          .findFirst()
//          .orElseThrow(
//              () -> new RuntimeException("Could not find insert statement for table: " + tableName)
//          );
//
//      mutationCoords = new PostgresLogMutationCoords(field.getName(), tableName,
//          insertStatement.getSql(), insertStatement.getParams());
    } else {
      throw new RuntimeException("Unknown mutation implementation: " + mutation.getCreateTopic());
    }

    mutations.add(mutationCoords);

  }

  @Override
  protected void visitUnknownObject(FieldDefinition atField, Optional<RelDataType> relDataType) {
  }

  @Override
  protected void visitScalar(ObjectTypeDefinition objectType, FieldDefinition atField, RelDataTypeField relDataTypeField) {
    //todo: walk into structured type to check all prop fetchers

    // we create PropertyDataFetchers for fields only when graphql field name is different from calcite field name
    if (hasVaryingCase(atField, relDataTypeField)) {
      FieldLookupQueryCoords fieldLookupCoords = FieldLookupQueryCoords.builder().parentType(objectType.getName())
          .fieldName(atField.getName()).columnName(relDataTypeField.getName()).build();
      queryCoords.add(fieldLookupCoords);
    }
  }

  @Override
  protected void visitQuery(ObjectTypeDefinition parentType, FieldDefinition atField, SqrlTableFunction tableFunction, TypeDefinitionRegistry registry) {
    // As we no more merge user provided graphQL schema with the inferred schema, we no more need to generate as many queries as the permutations of its arguments.
    // We now have a single executable query linked to the table function and already fully defined
    final ExecutableQuery executableQuery = tableFunction.getExecutableQuery();
    checkState(
        executableQuery instanceof ExecutableJdbcReadQuery,
        atField.getSourceLocation(),
        "This table function should be planned as an ExecutableJdbcReadQuery");
    final ExecutableJdbcReadQuery executableJdbcReadQuery = (ExecutableJdbcReadQuery) executableQuery;

    List<QueryParameterHandler> parameters = new ArrayList<>();
    for (FunctionParameter functionParameter : tableFunction.getParameters()) {
      final SqrlFunctionParameter parameter = (SqrlFunctionParameter) functionParameter;
      parameters.add(
          parameter.isParentField()
              ? new RootGraphqlModel.SourceParameter(parameter.getName())
              : new RootGraphqlModel.ArgumentParameter(parameter.getName()));
    }
    RootGraphqlModel.QueryBase queryBase;

    boolean hasLimitOrOffset = atField.getInputValueDefinitions().stream().map(InputValueDefinition::getName)
        .anyMatch(name -> name.equals(SchemaConstants.LIMIT) || name.equals(SchemaConstants.OFFSET));
    queryBase = new SqlQuery(executableJdbcReadQuery.getSql(), parameters,
        hasLimitOrOffset? PaginationType.LIMIT_AND_OFFSET: PaginationType.NONE,
        executableJdbcReadQuery.getDatabase());
    ArgumentLookupQueryCoords.ArgumentLookupQueryCoordsBuilder coordsBuilder = ArgumentLookupQueryCoords.builder()
        .parentType(parentType.getName()).fieldName(atField.getName());
    QueryWithArguments set = QueryWithArguments.builder().arguments(createArguments(atField))
            .query(queryBase).build();

    coordsBuilder.exec(set);
    queryCoords.add(coordsBuilder.build());
  }

  private static Set<Argument> createArguments(FieldDefinition field) {
    // create the arguements as they used to be created in QueryBuilderHelper
    Set<Argument> argumentSet = field.getInputValueDefinitions().stream()
            .map(input -> RootGraphqlModel.VariableArgument.builder()
                    .path(input.getName())
                    .value(null)
                    .build())
            .collect(Collectors.toSet());
    return argumentSet;
  }
}
