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
package com.datasqrl.graphql.server;

import graphql.Scalars;
import graphql.scalars.ExtendedScalars;
import graphql.scalars.datetime.DateTimeScalar;
import graphql.schema.Coercing;
import graphql.schema.GraphQLScalarType;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;

public class CustomScalars {

  public static final GraphQLScalarType DOUBLE =
      GraphQLScalarType.newScalar()
          .name("Float")
          .description("A Float with rounding applied")
          .coercing(
              new Coercing() {
                @Override
                public Object serialize(Object dataFetcherResult) {
                  if (dataFetcherResult instanceof Double doubleValue) {
                    var bd =
                        new BigDecimal(doubleValue)
                            .setScale(8, RoundingMode.HALF_UP)
                            .stripTrailingZeros();
                    // Convert back to normal readable number
                    return new BigDecimal(bd.toPlainString());
                  } else {
                    return Scalars.GraphQLFloat.getCoercing().serialize(dataFetcherResult);
                  }
                }

                @Override
                public Object parseValue(Object input) {
                  return Scalars.GraphQLFloat.getCoercing().parseValue(input);
                }

                @Override
                public Object parseLiteral(Object input) {
                  return Scalars.GraphQLFloat.getCoercing().parseLiteral(input);
                }
              })
          .build();

  // Extended scalars
  public static final GraphQLScalarType DATETIME = DateTimeScalar.INSTANCE;
  public static final GraphQLScalarType DATE = ExtendedScalars.Date;
  public static final GraphQLScalarType TIME = ExtendedScalars.LocalTime;
  public static final GraphQLScalarType JSON = ExtendedScalars.Json;
  public static final GraphQLScalarType LONG = ExtendedScalars.GraphQLLong;

  public static List<GraphQLScalarType> getExtendedScalars() {
    return List.of(DATETIME, DATE, TIME, JSON, LONG);
  }
}
