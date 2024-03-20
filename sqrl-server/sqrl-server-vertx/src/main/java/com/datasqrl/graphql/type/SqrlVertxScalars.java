package com.datasqrl.graphql.type;

import com.fasterxml.jackson.databind.ObjectMapper;
import graphql.Scalars;
import graphql.schema.Coercing;
import graphql.schema.GraphQLScalarType;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import lombok.SneakyThrows;

public class SqrlVertxScalars {

  static final ObjectMapper objectMapper = new ObjectMapper();

  public static final GraphQLScalarType JSON = GraphQLScalarType.newScalar().name("JSON").coercing(new Coercing<>() {
    @SneakyThrows
    @Override
    public Object serialize(Object dataFetcherResult) {
      if (dataFetcherResult instanceof String) {
        return objectMapper.readTree((String) dataFetcherResult);
      } else if (dataFetcherResult instanceof JsonObject) {
        // We can use JSONObjectAdapter instrumentation when we upgrade to graphql-java 20
        return ((JsonObject) dataFetcherResult).getMap();
      } else if (dataFetcherResult instanceof JsonArray) {
        return dataFetcherResult;
      }
      return Scalars.GraphQLString.getCoercing().serialize(dataFetcherResult);
    }

    @SneakyThrows
    @Override
    public Object parseValue(Object input) {
      return input;
    }

    @Override
    public Object parseLiteral(Object input) {
      return input;
    }
  }).build();
}
