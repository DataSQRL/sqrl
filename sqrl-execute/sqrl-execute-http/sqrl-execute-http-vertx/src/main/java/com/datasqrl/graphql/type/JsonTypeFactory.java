package com.datasqrl.graphql.type;

import com.datasqrl.graphql.server.GraphqlTypeFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import graphql.Scalars;
import graphql.schema.Coercing;
import graphql.schema.GraphQLScalarType;
import io.vertx.core.json.JsonObject;
import lombok.SneakyThrows;

public class JsonTypeFactory implements GraphqlTypeFactory {

  @Override
  public GraphQLScalarType create() {
    final ObjectMapper objectMapper = new ObjectMapper();
    return GraphQLScalarType.newScalar()
        .name("JSON")
        .coercing(new Coercing<>() {
          @SneakyThrows
          @Override
          public Object serialize(Object dataFetcherResult) {
            if (dataFetcherResult instanceof String) {
              return objectMapper.readTree((String)dataFetcherResult);
            } else if (dataFetcherResult instanceof JsonObject) {
              return ((JsonObject) dataFetcherResult).getMap();
            }
            return Scalars.GraphQLString.getCoercing().serialize(dataFetcherResult);
          }

          @SneakyThrows
          @Override
          public Object parseValue(Object input) {
            if (input instanceof String) {
              return objectMapper.readTree((String)input);
            } else if (input instanceof JsonObject) {
              return ((JsonObject) input).getMap();
            }
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
