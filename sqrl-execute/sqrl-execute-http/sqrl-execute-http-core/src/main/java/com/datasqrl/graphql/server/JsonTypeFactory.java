package com.datasqrl.graphql.server;

import graphql.Scalars;
import graphql.schema.Coercing;
import graphql.schema.GraphQLScalarType;

public class JsonTypeFactory implements GraphqlTypeFactory {

  @Override
  public GraphQLScalarType create() {
    return GraphQLScalarType.newScalar()
        .name("JSON")
        .coercing(new Coercing<>() {
          @Override
          public Object serialize(Object dataFetcherResult) {
            return Scalars.GraphQLString.getCoercing().serialize(dataFetcherResult);
          }

          @Override
          public Object parseValue(Object input) {
            return input;
          }

          @Override
          public Object parseLiteral(Object input) {
            return input;
          }
        })
        .build();
  }
}
