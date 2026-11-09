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
package com.datasqrl.server.stream;

import static graphql.schema.GraphQLTypeUtil.unwrapNonNull;
import static java.util.stream.Collectors.toSet;

import com.datasqrl.server.PaginationType;
import com.datasqrl.server.graphql.RootGraphQLModel.ArgumentLookupQueryCoords;
import com.datasqrl.server.graphql.RootGraphQLModel.QueryCoords;
import com.datasqrl.server.graphql.RootGraphQLModel.SqlQuery;
import com.datasqrl.server.jdbc.DatabaseType;
import com.datasqrl.server.util.CaseInsensitiveJsonDataFetcher;
import graphql.GraphQLException;
import graphql.language.Document;
import graphql.language.Field;
import graphql.language.OperationDefinition;
import graphql.language.OperationDefinition.Operation;
import graphql.language.SelectionSet;
import graphql.parser.Parser;
import graphql.parser.ParserEnvironment;
import graphql.parser.ParserOptions;
import graphql.schema.FieldCoordinates;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLNonNull;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLSchema;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Decides, before execution, whether a request can be streamed: a single query operation without
 * fragments or directives, selecting one top-level list field backed by a PostgreSQL query without
 * a page wrapper, whose selection holds only scalar columns of the row type.
 */
public class StreamEligibility {

  private static final Set<PaginationType> STREAMABLE_PAGINATION =
      Set.of(PaginationType.NONE, PaginationType.LIMIT_AND_OFFSET);

  private final GraphQLSchema schema;
  private final Set<String> streamableFields;

  public StreamEligibility(GraphQLSchema schema, List<QueryCoords> queries) {
    this.schema = schema;
    var queryType = schema.getQueryType().getName();
    this.streamableFields =
        queries.stream()
            .filter(coords -> coords.getParentType().equals(queryType))
            .filter(ArgumentLookupQueryCoords.class::isInstance)
            .map(ArgumentLookupQueryCoords.class::cast)
            .filter(coords -> isStreamable(coords.getExec().getQuery()))
            .map(QueryCoords::getFieldName)
            .collect(toSet());
  }

  private static boolean isStreamable(Object query) {
    return query instanceof SqlQuery sqlQuery
        && sqlQuery.getDatabase() == DatabaseType.POSTGRES
        && STREAMABLE_PAGINATION.contains(sqlQuery.getPagination());
  }

  public Optional<StreamPlan> plan(String query, @Nullable String operationName) {
    try {
      return plan(parse(query), operationName);
    } catch (GraphQLException e) {
      return Optional.empty();
    }
  }

  private static Document parse(String query) {
    return new Parser()
        .parseDocument(
            ParserEnvironment.newParserEnvironment()
                .document(query)
                .parserOptions(ParserOptions.getDefaultOperationParserOptions())
                .build());
  }

  private Optional<StreamPlan> plan(Document document, @Nullable String operationName) {
    if (document.getDefinitions().size() != 1
        || !(document.getDefinitions().get(0) instanceof OperationDefinition operation)
        || operation.getOperation() != Operation.QUERY
        || !operation.getDirectives().isEmpty()
        || (operationName != null && !operationName.equals(operation.getName()))) {
      return Optional.empty();
    }
    var selections = operation.getSelectionSet().getSelections();
    if (selections.size() != 1
        || !(selections.get(0) instanceof Field field)
        || !field.getDirectives().isEmpty()
        || field.getSelectionSet() == null
        || !streamableFields.contains(field.getName())) {
      return Optional.empty();
    }
    var fieldDefinition = schema.getQueryType().getFieldDefinition(field.getName());
    if (fieldDefinition == null
        || !(unwrapNonNull(fieldDefinition.getType()) instanceof GraphQLList list)
        || !(unwrapNonNull(list.getWrappedType()) instanceof GraphQLObjectType rowType)) {
      return Optional.empty();
    }
    return columns(rowType, field.getSelectionSet())
        .map(
            columns ->
                new StreamPlan(
                    responseKey(field),
                    fieldDefinition.getType() instanceof GraphQLNonNull,
                    field.getSourceLocation(),
                    columns));
  }

  private Optional<List<StreamedColumn>> columns(
      GraphQLObjectType rowType, SelectionSet selectionSet) {
    var columns = new ArrayList<StreamedColumn>();
    var responseKeys = new HashSet<String>();
    for (var selection : selectionSet.getSelections()) {
      if (!(selection instanceof Field field)
          || !field.getDirectives().isEmpty()
          || !field.getArguments().isEmpty()
          || field.getSelectionSet() != null
          || !responseKeys.add(responseKey(field))) {
        return Optional.empty();
      }
      var column = column(rowType, field);
      if (column.isEmpty()) {
        return Optional.empty();
      }
      columns.add(column.get());
    }
    return Optional.of(columns);
  }

  private Optional<StreamedColumn> column(GraphQLObjectType rowType, Field field) {
    var definition = rowType.getFieldDefinition(field.getName());
    if (definition == null) {
      return Optional.empty();
    }
    var fetcher =
        schema
            .getCodeRegistry()
            .getDataFetcher(FieldCoordinates.coordinates(rowType, definition), definition);
    if (!(fetcher instanceof CaseInsensitiveJsonDataFetcher propertyFetcher)) {
      return Optional.empty();
    }
    return StreamedColumn.of(
        responseKey(field), propertyFetcher.getPropertyName(), definition.getType());
  }

  private static String responseKey(Field field) {
    return field.getAlias() != null ? field.getAlias() : field.getName();
  }
}
