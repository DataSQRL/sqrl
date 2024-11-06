package com.datasqrl.graphql.server;

import graphql.Scalars;
import graphql.scalars.ExtendedScalars;
import graphql.scalars.datetime.DateScalar;
import graphql.scalars.datetime.DateTimeScalar;
import graphql.scalars.datetime.TimeScalar;
import graphql.schema.Coercing;
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
            //Convert back to normal readable number
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


  public static final GraphQLScalarType DATETIME =  ExtendedScalars.DateTime;
  public static final GraphQLScalarType DATE =  ExtendedScalars.Date;
  public static final GraphQLScalarType TIME = ExtendedScalars.LocalTime;
}