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
package com.datasqrl.ai.converter;

import graphql.language.OperationDefinition.Operation;
import java.util.Arrays;
import java.util.function.BiPredicate;
import lombok.Builder;
import lombok.Value;

/** Configuration class for {@link GraphQLSchemaConverter}. */
@Value
@Builder
public class GraphQLSchemaConverterConfig {

  public static final GraphQLSchemaConverterConfig DEFAULT =
      GraphQLSchemaConverterConfig.builder().build();

  /** Filter for selecting which operations to convert */
  @Builder.Default BiPredicate<Operation, String> operationFilter = (op, name) -> true;

  /** The maximum depth of conversion for operations that have nested types */
  @Builder.Default int maxDepth = 3;

  /**
   * Returns an operations filter that filters out all operations which start with the given list of
   * prefixes.
   *
   * @param prefixes
   * @return
   */
  public static BiPredicate<Operation, String> ignorePrefix(String... prefixes) {
    final String[] prefixesLower =
        Arrays.stream(prefixes).map(String::trim).map(String::toLowerCase).toArray(String[]::new);
    return (op, name) ->
        Arrays.stream(prefixesLower)
            .noneMatch(prefixLower -> name.trim().toLowerCase().startsWith(prefixLower));
  }
}
