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

import static org.assertj.core.api.Assertions.assertThat;

import com.datasqrl.server.operation.GraphQLQuery;
import graphql.ErrorType;
import graphql.ExecutionInput;
import graphql.execution.preparsed.PreparsedDocumentEntry;
import graphql.language.OperationDefinition.Operation;
import graphql.parser.Parser;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class OperationOnlyPreparsedDocumentProviderTest {

  private static final String CONFIGURED_QUERY = "query GetGreeting { greeting }";

  private final OperationOnlyPreparsedDocumentProvider provider =
      new OperationOnlyPreparsedDocumentProvider(
          List.of(new GraphQLQuery(CONFIGURED_QUERY, "GetGreeting", Operation.QUERY)));

  @Test
  void givenConfiguredOperationName_whenQueryDiffers_thenExecutesConfiguredQuery() {
    var executionInput =
        ExecutionInput.newExecutionInput().query("query GetGreeting { other: greeting }").build();
    var capturedInput = new AtomicReference<ExecutionInput>();

    var result =
        provider
            .getDocumentAsync(
                executionInput,
                input -> {
                  capturedInput.set(input);
                  return new PreparsedDocumentEntry(Parser.parse(input.getQuery()));
                })
            .join();

    assertThat(result.hasErrors()).isFalse();
    assertThat(capturedInput.get().getQuery()).isEqualTo(CONFIGURED_QUERY);
    assertThat(capturedInput.get().getOperationName()).isEqualTo("GetGreeting");
  }

  @Test
  void givenUnknownOperation_whenRequested_thenRejectsWithoutParsingIt() {
    var executionInput =
        ExecutionInput.newExecutionInput().query("query Other { greeting }").build();
    var parsed = new AtomicBoolean();

    var result =
        provider
            .getDocumentAsync(
                executionInput,
                input -> {
                  parsed.set(true);
                  return new PreparsedDocumentEntry(Parser.parse(input.getQuery()));
                })
            .join();

    assertThat(parsed).isFalse();
    assertThat(result.hasErrors()).isTrue();
    assertThat(result.getErrors())
        .singleElement()
        .extracting(error -> error.getMessage())
        .isEqualTo("Unknown GraphQL operation: Other");
  }

  @Test
  void givenAnonymousOperation_whenRequested_thenRejectsIt() {
    var executionInput = ExecutionInput.newExecutionInput().query("{ greeting }").build();

    var result =
        provider
            .getDocumentAsync(
                executionInput, input -> new PreparsedDocumentEntry(Parser.parse(input.getQuery())))
            .join();

    assertThat(result.hasErrors()).isTrue();
  }

  @Test
  void givenInvalidSyntax_whenRequested_thenReturnsInvalidSyntaxError() {
    var executionInput = ExecutionInput.newExecutionInput().query("query GetGreeting {").build();

    var result =
        provider
            .getDocumentAsync(
                executionInput, input -> new PreparsedDocumentEntry(Parser.parse(input.getQuery())))
            .join();

    assertThat(result.hasErrors()).isTrue();
    assertThat(result.getErrors())
        .singleElement()
        .extracting(error -> error.getErrorType())
        .isEqualTo(ErrorType.InvalidSyntax);
  }
}
