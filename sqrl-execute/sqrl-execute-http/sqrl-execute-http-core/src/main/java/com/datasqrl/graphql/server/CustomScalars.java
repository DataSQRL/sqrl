package com.datasqrl.graphql.server;

import graphql.Scalars;
import graphql.schema.Coercing;
import graphql.schema.CoercingSerializeException;
import graphql.schema.GraphQLScalarType;
import java.math.BigDecimal;
import java.math.RoundingMode;

public class CustomScalars {

  public static final GraphQLScalarType Double = GraphQLScalarType.newScalar()
      .name("Float")
      .description("A Float with rounding applied")
      .coercing(new Coercing() {
        @Override
        public Object serialize(Object dataFetcherResult) {
          if (dataFetcherResult instanceof Double) {
            Double doubleValue = (Double) dataFetcherResult;
            BigDecimal bd = new BigDecimal(doubleValue)
                .setScale(8, RoundingMode.HALF_UP)
                .stripTrailingZeros();
            return bd;
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
}