package com.datasqrl.graphql.server;

import graphql.Scalars;
import graphql.schema.Coercing;
import graphql.schema.CoercingSerializeException;
import graphql.schema.GraphQLScalarType;
import java.math.BigDecimal;
import java.math.RoundingMode;

public class CustomScalars {

  public static final GraphQLScalarType Double = GraphQLScalarType.newScalar()
      .name("Double")
      .description("A Double with rounding applied")
      .coercing(new Coercing() {
        @Override
        public Object serialize(Object dataFetcherResult) {
          if (dataFetcherResult instanceof Double) {
            Double doubleValue = (Double) dataFetcherResult;
            BigDecimal bd = new BigDecimal(doubleValue);
            bd = bd.setScale(8, RoundingMode.HALF_UP)
                .stripTrailingZeros();
            return bd.doubleValue();
          } else {
            throw new CoercingSerializeException(
                "Unable to serialize " + dataFetcherResult + " as a double");
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