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
package com.datasqrl.server.graphql;

import com.datasqrl.server.operation.GraphQLQuery;
import graphql.ErrorType;
import graphql.ExecutionInput;
import graphql.GraphqlErrorBuilder;
import graphql.execution.preparsed.PreparsedDocumentEntry;
import graphql.execution.preparsed.PreparsedDocumentProvider;
import graphql.language.OperationDefinition;
import graphql.parser.InvalidSyntaxException;
import graphql.parser.Parser;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Restricts GraphQL execution to configured named operations and executes their stored query text.
 */
public class OperationOnlyPreparsedDocumentProvider implements PreparsedDocumentProvider {

  private final Map<String, String> operations;

  public OperationOnlyPreparsedDocumentProvider(Collection<GraphQLQuery> operations) {
    this.operations = new LinkedHashMap<>();
    for (var operation : operations) {
      this.operations.putIfAbsent(operation.queryName(), operation.query());
    }
  }

  @Override
  public CompletableFuture<PreparsedDocumentEntry> getDocumentAsync(
      ExecutionInput executionInput,
      Function<ExecutionInput, PreparsedDocumentEntry> parseAndValidateFunction) {

    final String operationName;
    try {
      operationName = getOperationName(executionInput);
    } catch (InvalidSyntaxException e) {
      return CompletableFuture.completedFuture(
          new PreparsedDocumentEntry(e.toInvalidSyntaxError()));
    }

    if (operations.containsKey(operationName)) {
      var configuredInput =
          executionInput.transform(
              builder -> builder.query(operations.get(operationName)).operationName(operationName));

      return CompletableFuture.completedFuture(parseAndValidateFunction.apply(configuredInput));
    }

    return CompletableFuture.completedFuture(
        new PreparsedDocumentEntry(
            GraphqlErrorBuilder.newError()
                .message(
                    operationName == null
                        ? "A named GraphQL operation is required"
                        : "Unknown GraphQL operation: " + operationName)
                .errorType(ErrorType.OperationNotSupported)
                .build()));
  }

  private String getOperationName(ExecutionInput executionInput) {
    var document = Parser.parse(executionInput.getQuery());
    var requestedName = executionInput.getOperationName();
    if (requestedName != null) {
      return document.getOperationDefinition(requestedName).isPresent() ? requestedName : null;
    }

    var definitions = document.getDefinitionsOfType(OperationDefinition.class);

    return definitions.size() == 1 ? definitions.get(0).getName() : null;
  }
}
