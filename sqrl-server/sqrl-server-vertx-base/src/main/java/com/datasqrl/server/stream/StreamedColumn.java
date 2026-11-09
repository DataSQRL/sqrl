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

import graphql.schema.GraphQLList;
import graphql.schema.GraphQLNonNull;
import graphql.schema.GraphQLOutputType;
import graphql.schema.GraphQLScalarType;
import java.util.Optional;

/** A scalar column of the streamed rows, written under {@code responseKey}. */
public record StreamedColumn(
    String responseKey,
    String columnName,
    GraphQLScalarType scalar,
    boolean nonNull,
    boolean list,
    boolean itemNonNull) {

  static Optional<StreamedColumn> of(
      String responseKey, String columnName, GraphQLOutputType fieldType) {
    var nonNull = fieldType instanceof GraphQLNonNull;
    var type = unwrapNonNull(fieldType);
    if (type instanceof GraphQLScalarType scalar) {
      return Optional.of(
          new StreamedColumn(responseKey, columnName, scalar, nonNull, false, false));
    }
    if (type instanceof GraphQLList list
        && unwrapNonNull(list.getWrappedType()) instanceof GraphQLScalarType scalar) {
      var itemNonNull = list.getWrappedType() instanceof GraphQLNonNull;
      return Optional.of(
          new StreamedColumn(responseKey, columnName, scalar, nonNull, true, itemNonNull));
    }
    return Optional.empty();
  }
}
