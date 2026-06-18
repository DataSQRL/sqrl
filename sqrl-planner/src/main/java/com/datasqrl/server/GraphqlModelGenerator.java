/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasqrl.server;

import static com.datasqrl.server.util.GraphqlCheckUtil.checkState;
import static com.datasqrl.server.util.GraphqlSchemaUtil.hasVaryingCase;

import com.datasqrl.engine.database.relational.ExecutableJdbcReadQuery;
import com.datasqrl.engine.log.kafka.KafkaLogEngine;
import com.datasqrl.engine.log.kafka.KafkaQuery;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.planner.dag.plan.MutationTable;
import com.datasqrl.planner.parser.AccessModifier;
import com.datasqrl.planner.tables.SqrlFunctionParameter;
import com.datasqrl.planner.tables.SqrlTableFunction;
import com.datasqrl.server.graphql.RootGraphQLModel;
import com.datasqrl.server.graphql.RootGraphQLModel.Argument;
import com.datasqrl.server.graphql.RootGraphQLModel.ArgumentLookupQueryCoords;
import com.datasqrl.server.graphql.RootGraphQLModel.ArgumentParameter;
import com.datasqrl.server.graphql.RootGraphQLModel.ComputedParameter;
import com.datasqrl.server.graphql.RootGraphQLModel.FieldLookupQueryCoords;
import com.datasqrl.server.graphql.RootGraphQLModel.KafkaMutationCoords;
import com.datasqrl.server.graphql.RootGraphQLModel.KafkaSubscriptionCoords;
import com.datasqrl.server.graphql.RootGraphQLModel.MetadataParameter;
import com.datasqrl.server.graphql.RootGraphQLModel.MutationCoords;
import com.datasqrl.server.graphql.RootGraphQLModel.ParentParameter;
import com.datasqrl.server.graphql.RootGraphQLModel.QueryCoords;
import com.datasqrl.server.graphql.RootGraphQLModel.QueryParameterHandler;
import com.datasqrl.server.graphql.RootGraphQLModel.QueryWithArguments;
import com.datasqrl.server.graphql.RootGraphQLModel.SqlQuery;
import com.datasqrl.server.graphql.RootGraphQLModel.SubscriptionCoords;
import com.datasqrl.server.jdbc.SchemaConstants;
import com.datasqrl.server.util.GraphqlSchemaUtil;
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

/** Returns a set of table functions that satisfy a graphql schema */
@Getter
public class GraphqlModelGenerator extends GraphqlSchemaWalker {

  List<QueryCoords> queryCoords = new ArrayList<>();
  List<MutationCoords> mutations = new ArrayList<>();
  List<SubscriptionCoords> subscriptions = new ArrayList<>();
  private final ErrorCollector errorCollector;

  public GraphqlModelGenerator(
      List<SqrlTableFunction> tableFunctions,
      List<MutationTable> mutations,
      ErrorCollector errorCollector) {
    super(tableFunctions, mutations);
    this.errorCollector = errorCollector;
  }

  @Override
  protected void visitSubscription(
      FieldDefinition atField, SqrlTableFunction tableFunction, TypeDefinitionRegistry registry) {
    Preconditions.checkArgument(
        tableFunction.getVisibility().access() == AccessModifier.SUBSCRIPTION);
    final var executableQuery = tableFunction.getExecutableQuery();

    var fieldName = atField.getName();
    if (executableQuery instanceof KafkaQuery kafkaQuery) {
      var filters =
          kafkaQuery.getFilterColumnNames().entrySet().stream()
              .collect(Collectors.toMap(Entry::getKey, e -> convert(e.getValue())));

      subscriptions.add(
          new KafkaSubscriptionCoords(fieldName, kafkaQuery.getTopicName(), Map.of(), filters));

    } else {
      throw new UnsupportedOperationException("Unsupported subscription query: " + executableQuery);
    }
  }

  @Override
  protected void visitMutation(
      FieldDefinition atField, TypeDefinitionRegistry registry, MutationTable mutation) {

    var computedCols = mutation.getComputedColumns();
    var returnList = GraphqlSchemaUtil.isListType(atField.getType());

    if (mutation.getCreateTable() instanceof KafkaLogEngine.Table mutationTopic) {
      mutations.add(
          new KafkaMutationCoords(
              atField.getName(),
              returnList,
              mutationTopic.topicName(),
              mutationTopic.messageKeys(),
              computedCols,
              mutation.getInsertType() == MutationInsertType.TRANSACTION,
              Map.of()));
    } else {
      throw new RuntimeException(
          "Unsupported mutation implementation: " + mutation.getCreateTable());
    }
  }

  @Override
  protected void visitUnknownObject(FieldDefinition atField, Optional<RelDataType> relDataType) {}

  @Override
  protected void visitScalar(
      ObjectTypeDefinition objectType, FieldDefinition atField, RelDataTypeField relDataTypeField) {
    // todo: walk into structured type to check all prop fetchers

    // we create PropertyDataFetchers for fields only when graphql field name is different from
    // calcite field name
    if (hasVaryingCase(atField, relDataTypeField)) {
      var fieldLookupCoords =
          FieldLookupQueryCoords.builder()
              .parentType(objectType.getName())
              .fieldName(atField.getName())
              .columnName(relDataTypeField.getName())
              .build();
      queryCoords.add(fieldLookupCoords);
    }
  }

  @Override
  protected void visitQuery(
      ObjectTypeDefinition parentType,
      FieldDefinition atField,
      SqrlTableFunction tableFunction,
      TypeDefinitionRegistry registry) {
    // As we no more merge user provided graphQL schema with the inferred schema, we no more need to
    // generate as many queries as the permutations of its arguments.
    // We now have a single executable query linked to the table function and already fully defined
    final var executableQuery = tableFunction.getExecutableQuery();
    checkState(
        executableQuery instanceof ExecutableJdbcReadQuery,
        atField.getSourceLocation(),
        "This table function should be planned as an ExecutableJdbcReadQuery");
    final var executableJdbcReadQuery = (ExecutableJdbcReadQuery) executableQuery;

    List<QueryParameterHandler> parameters =
        tableFunction.getParameters().stream().map(GraphqlModelGenerator::convert).toList();
    RootGraphQLModel.QueryBase queryBase;

    var hasLimitOrOffset =
        atField.getInputValueDefinitions().stream()
            .map(InputValueDefinition::getName)
            .anyMatch(
                name -> name.equals(SchemaConstants.LIMIT) || name.equals(SchemaConstants.OFFSET));
    queryBase =
        new SqlQuery(
            executableJdbcReadQuery.getSql(),
            parameters,
            hasLimitOrOffset ? PaginationType.LIMIT_AND_OFFSET : PaginationType.NONE,
            executableJdbcReadQuery.getCacheDuration().toMillis(),
            executableJdbcReadQuery.getDatabase());
    var coordsBuilder =
        ArgumentLookupQueryCoords.builder()
            .parentType(parentType.getName())
            .fieldName(atField.getName());
    var set =
        QueryWithArguments.builder().arguments(createArguments(atField)).query(queryBase).build();

    coordsBuilder.exec(set);
    queryCoords.add(coordsBuilder.build());
  }

  private static QueryParameterHandler convert(FunctionParameter fnParam) {
    final var sqrlParam = (SqrlFunctionParameter) fnParam;

    if (sqrlParam.isParentField()) {
      return new ParentParameter(sqrlParam.getName());
    }

    if (sqrlParam.isMetadata()) {
      return new MetadataParameter(sqrlParam.getMetadata().get());
    }

    if (sqrlParam.isFunction()) {
      return new ComputedParameter(sqrlParam.getFunction().get().getFunctionId());
    }

    return new ArgumentParameter(
        sqrlParam.getName(), sqrlParam.getRelDataType().getSqlTypeName().getName());
  }

  private static Set<Argument> createArguments(FieldDefinition field) {
    // create the arguements as they used to be created in QueryBuilderHelper
    return field.getInputValueDefinitions().stream()
        .map(
            input ->
                RootGraphQLModel.VariableArgument.builder()
                    .path(input.getName())
                    .value(null)
                    .build())
        .collect(Collectors.toSet());
  }
}
