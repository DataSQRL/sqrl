package com.datasqrl.graphql.server;

import graphql.schema.GraphQLScalarType;

public interface GraphqlTypeFactory {
  GraphQLScalarType create();
}
